#include <mpi.h>
#include <unistd.h> // for sleep()
#include <assert.h> // for assert()
#include "state/state.h"

#define RESPONSE_THRESHOLD_IN_L (1000)
#define PRECALCULATED_DISTANCES (10000)
#define STEP_TIME_WARMUP_TESTS  (100)
#define STEP_TIME_MEASURE_TESTS (1000000)

typedef enum work_action {
	WORK_ACTION_RECV,
	WORK_ACTION_SEND,
	WORK_ACTION_SKIP, /* Recieved earlier */
	WORK_ACTION_DONE
} action_e;

typedef enum work_reaction {
	WORK_REACTION_NONE,
	WORK_REACTION_SEND_KEEPALIVE,
	WORK_REACTION_ASSUME_DEAD
} reaction_e;

typedef struct work_step {
	node_id node;      /* Which node is next? */
	action_e act;      /* What to do with the next node? */
	step_num deadline; /* How long to wait on it? */
	reaction_e react;  /* What to do if unresponsive? (after deadline) */
	unsigned distance; /* How fat is that node? */
	int ack_needed;    /* Is he expecting an ACK message? */
} step_t;

typedef enum work_msg {
	WORK_MSG_DATA,
	WORK_MSG_KA,
	WORK_MSG_ACK
} msg_e;

struct pkt {
	msg_e type;
	char data[0]; // TODO: support different message sizes!
};

struct order {
    enum comm_graph_direction_type direction;
    action_e action;
} fast_tree_order[] = {
		/* First - wait for children */
        {COMM_GRAPH_CHILDREN,       WORK_ACTION_RECV},/* Death -> nothing */
        {COMM_GRAPH_EXTRA_CHILDREN, WORK_ACTION_RECV},/* Death -> nothing */

		/* Then - send to fathers (up the tree) */
        {COMM_GRAPH_FATHERS,        WORK_ACTION_SEND},/* Death -> zero send_idx */
        {COMM_GRAPH_EXTRA_FATHERS,  WORK_ACTION_SEND},/* Death -> zero send_idx */

		/* Next - wait for results (for verbose mode) */
        {COMM_GRAPH_FATHERS,        WORK_ACTION_RECV},/* Death -> goto COMM_GRAPH_EXTRA_FATHERS, zero send_idx */
        {COMM_GRAPH_EXTRA_FATHERS,  WORK_ACTION_RECV},/* Death -> goto COMM_GRAPH_EXTRA_FATHERS, zero send_idx */

		/* Finally - send to children (down the tree) */
		{COMM_GRAPH_CHILDREN,       WORK_ACTION_SEND},/* Death -> zero send_idx */
        {COMM_GRAPH_EXTRA_CHILDREN, WORK_ACTION_SEND},/* Death -> zero send_idx */

		/* DONE */
        {COMM_GRAPH_EXCLUDE,        WORK_ACTION_DONE}
};

static inline int fast_tree_recv(struct pkt *incoming, node_id *sender, unsigned tag)
{
	/* Check for matching message arrival */
	int arrived;
	MPI_Status status;
	MPI_Message message;
	MPI_Improbe(MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &arrived, &message, &status);
	if (!arrived) {
		return DONE;
	}

	*sender = status.MPI_SOURCE;
	return MPI_Mrecv(incoming, sizeof(struct pkt), MPI_UNSIGNED_CHAR, &message, MPI_STATUS_IGNORE);
}

static inline int fast_tree_send(node_id src, msg_e msg, node_id dst, unsigned tag)
{
	struct pkt sent = { .type = msg };
	int ret = MPI_Send(&sent, sizeof(sent), MPI_UNSIGNED_CHAR, dst, tag, MPI_COMM_WORLD);
	if (ret) {
		return ERROR;
	}
	return OK;
}

static inline unsigned long fast_tree_timer()
{
    unsigned low, high;
    asm volatile ("rdtsc" : "=a" (low), "=d" (high));
    return ((unsigned long)high << 32) | (unsigned long)low;
}

static inline unsigned long fast_tree_measure_step(node_id rank, node_id size)
{
	struct pkt dummy;
	unsigned long i, time;
	node_id shifted_rank = (rank + 1) % size; /* Either 0 or 1 */
	if (shifted_rank < 2) {
		node_id peer = (rank == 0) ? size - 1 : 0;

		if (rank == 0) {
			fast_tree_send(0, 0, peer, 0);
		}
		for (i = 0; i < STEP_TIME_WARMUP_TESTS; i++) {
			MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			fast_tree_recv(&dummy, &shifted_rank, 0);
			fast_tree_send(0, 0, peer, 0);
		}

		time = fast_tree_timer();
		for (i = 0; i < STEP_TIME_MEASURE_TESTS; i++) {
			MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			fast_tree_recv(&dummy, &shifted_rank, 0);
			fast_tree_send(0, 0, peer, 0);
		}
		if (rank != 0) {
			MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			fast_tree_recv(&dummy, &shifted_rank, 0);
		}
	}

	time = (fast_tree_timer() - time) / STEP_TIME_MEASURE_TESTS;
	MPI_Bcast(&time, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
	return time;
}

static inline unsigned fast_tree_randomized_service_distance(topology_spec_t *spec)
{
    unsigned distance = 0;
    float rand = FLOAT_RANDOM(spec);
    float tester = 1.0;

    while (rand < tester) {
        tester /= 2.0;
        distance++;
    }
    return distance;
}

static inline step_t* fast_tree_plan(step_t *old_plan, comm_graph_t *graph,
		node_id my_rank, int is_father_dead)
{
	step_t *new_plan_iter, *new_plan;
	comm_graph_node_t *my_node = &graph->nodes[my_rank];
	unsigned plan_size = my_node->directions[COMM_GRAPH_CHILDREN]->node_count +
			             my_node->directions[COMM_GRAPH_EXTRA_CHILDREN]->node_count +
						 my_node->directions[COMM_GRAPH_FATHERS]->node_count +
						 my_node->directions[COMM_GRAPH_EXTRA_FATHERS]->node_count;
	new_plan = new_plan_iter = malloc(((2 * plan_size) + 1) * sizeof(step_t));
	if (!new_plan) {
		return NULL;
	}

	// TODO: use is_father_dead ?

	node_id node_iter;
	struct order* steps_iter;
	step_t *old_plan_iter = old_plan ? old_plan : NULL;
	for (steps_iter = fast_tree_order; steps_iter->action != WORK_ACTION_DONE; steps_iter++) {
		enum comm_graph_direction_type dir_type = steps_iter->direction;
        comm_graph_direction_ptr_t dir_ptr = my_node->directions[dir_type];
        for (node_iter = 0; node_iter < dir_ptr->node_count; new_plan_iter++, node_iter++) {
        	/* Some common initialization */
        	new_plan_iter->node = dir_ptr->nodes[node_iter];
        	new_plan_iter->ack_needed = 0;
        	new_plan_iter->distance = 1;

        	/* Define a deadline on every recv() request */
        	if (steps_iter->action == WORK_ACTION_RECV) {
        		new_plan_iter->react = WORK_REACTION_SEND_KEEPALIVE;
        		switch (dir_type) {
        		case COMM_GRAPH_CHILDREN:
        			new_plan_iter->deadline = my_node->data_eta[DATA_ETA_SUBTREE];
        			break;
        		case COMM_GRAPH_FATHERS:
        			new_plan_iter->deadline = my_node->data_eta[DATA_ETA_FULL_TREE];
        			// TODO: recalculate
        			break;
        			// TODO: add deadline in re-plan code
        		default:
        			break;
        		}
        	} else {
        		new_plan_iter->react = WORK_REACTION_NONE;
        	}

        	if ((old_plan_iter) &&
        		(old_plan_iter->node == new_plan_iter->node) &&
        		(old_plan_iter->act == WORK_ACTION_SKIP)) {
        		new_plan_iter->act = WORK_ACTION_SKIP;
        		old_plan_iter++;
        	} else {
        		new_plan_iter->act = steps_iter->action;
        		old_plan_iter = NULL;
        	}
        }
	}

	new_plan_iter->act = WORK_ACTION_DONE;
	if (old_plan) {
		free(old_plan);
	}
	return new_plan;
}

int tree_fix_graph(void *ctx, comm_graph_t *graph, node_id my_rank,
		tree_recovery_method_t recovery, node_id source, int source_is_dead,
		int *is_father_dead);

static inline step_t* fast_tree_replan(step_t *plan, comm_graph_t *graph,
		node_id my_rank, tree_recovery_method_t recovery,
		node_id source, int source_is_dead)
{
	int is_father_dead = 0;
	int ret = tree_fix_graph(NULL, graph, my_rank, recovery, source,
			source_is_dead, &is_father_dead);
	if (ret) {
		return NULL;
	}

	/* Create a new plan (to replace the original) */
	return fast_tree_plan(plan, graph, my_rank, is_father_dead);
}

static inline step_t* fast_tree_find(step_t *next, node_id node)
{
	do {
		if (next->node == node) {
			return next;
		}
		next++;
	} while (next->act != WORK_ACTION_DONE);
	return NULL;
}

int tree_build(topology_spec_t *spec, comm_graph_t **graph);

unsigned long g_ticks_per_step = 0;

int sim_fast_tree(topology_spec_t *spec, raw_stats_t *stats)
{
	/* First, build one global graph */
	comm_graph_t *graph;
	node_id my_rank = spec->my_rank;
	int ret = tree_build(spec, &graph);
	if (ret != OK) {
		return ret;
	}

	/* Use the graph to prepare a plan */
	step_t *plan = fast_tree_plan(NULL, graph, my_rank, 0);
	if (!plan) {
		return ERROR;
	}

	/* Create some pre-calculated service distance values */
	unsigned precalcs_idx, precalcs_cnt = PRECALCULATED_DISTANCES;
	unsigned *precalcs = malloc(precalcs_cnt * sizeof(unsigned));
	for (precalcs_idx = 0; precalcs_idx < precalcs_cnt; precalcs_idx++) {
		precalcs[precalcs_idx] = fast_tree_randomized_service_distance(spec);
	}
	precalcs_idx = 0;

	/* Measure the latency */
	if (g_ticks_per_step == 0) {
		g_ticks_per_step = fast_tree_measure_step(my_rank, spec->node_count);
		if ((my_rank == 0) && (spec->verbose)) {
			unsigned long second_in_ticks = fast_tree_timer();
			sleep(1);
			second_in_ticks = fast_tree_timer() - second_in_ticks;
			printf("Measured latency: %f microseconds.\n",
					second_in_ticks / (1000000.0 * g_ticks_per_step));
		}
	}

	/* start the clock! */
	MPI_Barrier(MPI_COMM_WORLD);
	unsigned long start = fast_tree_timer();

	/* Main loop - go through the plan */
	struct pkt msg;
	node_id sender;
	step_t *search, *next = plan;
	while (ret == OK) {
		/* Determine the service distance to serve in this iteration */
		unsigned service_distance = (precalcs_idx < precalcs_cnt) ?
				precalcs[precalcs_idx++] :
				fast_tree_randomized_service_distance(spec);

		/* Determine which step are we at (approximate) */
		step_num now = (fast_tree_timer() - start) / g_ticks_per_step;

any_distance:
		switch (next->act) {
		case WORK_ACTION_RECV:
			/* Read next message */
			ret = fast_tree_recv(&msg, &sender, service_distance);
			if (ret == DONE) {
				ret = OK;
				break; /* No incoming messages */
			} else if (ret) {
				return ERROR;
			}

			/* Is this what I'm waiting for? */
			if ((next->node == sender) && (msg.type == WORK_MSG_DATA)) {
				//printf("#%li got DATA from #%li\n", my_rank, sender);
				next->act = WORK_ACTION_SKIP;
				next->react = WORK_REACTION_NONE;
				next->ack_needed = 1;
				next++;
				continue;
			}

			/* Find the sender and handle it */
			assert(sender != my_rank);
			search = fast_tree_find(next, sender);
			if (search == NULL) {
				//printf("Rank #%li got unexpected #%li!\n", my_rank, sender);
				/* Unexpected sender! means somebody else died... */
				next = plan = fast_tree_replan(plan, graph, my_rank,
						spec->topology.tree.recovery, sender, 0);
				if (!plan) {
					return ERROR;
				}
			} else switch (msg.type) {
			case WORK_MSG_DATA:
				//printf("#%li got DATA from #%li\n", my_rank, sender);
				assert(search->act == WORK_ACTION_RECV);
				search->act = WORK_ACTION_SKIP;
				search->react = WORK_REACTION_NONE;
				search->ack_needed = 1;
				break;

			case WORK_MSG_ACK:
				//printf("#%li got ACK from #%li\n", my_rank, sender);
				search->deadline = now + RESPONSE_THRESHOLD_IN_L;
				search->react = WORK_REACTION_SEND_KEEPALIVE;
				search->ack_needed = 0;
				break;

			case WORK_MSG_KA:
				//printf("#%li got KA from #%li\n", my_rank, sender);
				search->ack_needed = 1;
				break;
			}
			continue;

		case WORK_ACTION_SEND:
			/* Send data on the tree (and move to the next step of the plan) */
			ret = fast_tree_send(my_rank, WORK_MSG_DATA, next->node, next->distance);
			//printf("#%li sent DATA to #%li (tag=%i)\n", my_rank, next->node, next->distance);
			/* Make sure this action is skipped from now on */
			next->act = WORK_ACTION_SKIP;
			/* no break */
		case WORK_ACTION_SKIP:
			/* Skip over a previously received message */
			next++;
			continue;

		case WORK_ACTION_DONE:
			/* Collective operation is complete! */
			stats->last_step_counter = now;
			free(precalcs);
			free(plan);
			return OK;
		}

		/* Find a target */
		search = next;
		do {
			if ((service_distance == MPI_ANY_TAG) ||
				(search->distance == service_distance)) {
				if (search->ack_needed) {
					/* Send back and ACK message */
					ret = fast_tree_send(my_rank, WORK_MSG_ACK, search->node, search->distance);
					//printf("#%li sent ACK to #%li (tag=%i)\n", my_rank, search->node, search->distance);
					search->ack_needed = 0;
					service_distance = MPI_ANY_TAG; /* Avoid wild-card run */
					break;
				} else if ((search->react != WORK_REACTION_NONE) && (search->deadline < now)) {
					/* Some deadline expired - reaction is due! */
					switch (search->react) {
					case WORK_REACTION_NONE:
						break; /* Never happens, see if-condition */
					case WORK_REACTION_SEND_KEEPALIVE:
						ret = fast_tree_send(my_rank, WORK_MSG_KA, search->node, search->distance);
						//printf("#%li sent KA to #%li (tag=%i)\n", my_rank, search->node, search->distance);
						search->deadline = now + RESPONSE_THRESHOLD_IN_L;
						search->react = WORK_REACTION_ASSUME_DEAD;
						break;
					case WORK_REACTION_ASSUME_DEAD:
						plan = next = fast_tree_replan(plan, graph, my_rank,
								spec->topology.tree.recovery, search->node, 1);
						if (!plan) {
							return ERROR;
						}
						break;
					}
					break;
				}
			}
			search++;
		} while (search->act != WORK_ACTION_DONE);

		/* If no packet has been found */
		if (service_distance != MPI_ANY_TAG) {
			service_distance = MPI_ANY_TAG;
			goto any_distance;
		}
	}

	/* Some error occurred - return it */
	return ERROR;
}
