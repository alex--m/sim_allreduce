#include <math.h>
#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

#define TREE_PACKET_CHOSEN (1)
#define TREE_SERVICE_CYCLE_LENGTH ()

typedef unsigned tree_distance_t;
typedef unsigned tree_step_t;

typedef struct tree_contact {
	node_id node;             /* His node-identifier in the graph */
	tree_distance_t distance; /* His distance from me in the graph */
	step_num last_seen;       /* Last time he sent me anything */
	step_num last_sent;       /* Last time I sent him anything */
	step_num timeout;         /* How long until I consider him dead */
#define TIMEOUT_NEVER ((step_num)-1)
	step_num his_timeout;     /* How long until he considers me dead */
} tree_contact_t;

typedef struct tree_context {
    unsigned char *my_bitfield;
    step_num *step_index;
    node_id my_node;
    step_num latency;
    enum tree_service_cycle_method service_method;
    unsigned random_seed;

    unsigned next_wait_index;
    unsigned next_send_index;

    unsigned contacts_size;
    unsigned contacts_used;
    tree_contact_t *contacts;
} tree_context_t;


enum tree_msg_type {
    TREE_MSG_DATA          = 0,
#define TREE_MSG_DATA(distance) (TREE_MSG_DATA          + TREE_MSG_MAX * distance)
    TREE_MSG_KEEPALIVE     = 1,
#define TREE_MSG_KA(distance)   (TREE_MSG_KEEPALIVE     + TREE_MSG_MAX * distance)
    TREE_MSG_KEEPALIVE_ACK = 2,
#define TREE_MSG_ACK(distance)  (TREE_MSG_KEEPALIVE_ACK + TREE_MSG_MAX * distance)
	TREE_MSG_MAX           = 3,
};

enum tree_action {
    TREE_SEND,
    TREE_RECV,
    TREE_WAIT
};

struct order {
    enum comm_graph_direction_type direction;
    enum tree_action action;
};

size_t tree_ctx_size() {
    return sizeof(tree_context_t);
}

int tree_start(topology_spec_t *spec, comm_graph_t *graph, tree_context_t *ctx)
{
    comm_graph_node_t *my_node = &graph->nodes[spec->my_rank];
    comm_graph_direction_ptr_t dir_ptr;
    unsigned idx;

	memset(ctx, 0, sizeof(*ctx));
	ctx->my_node = spec->my_rank;
	ctx->latency = spec->latency;
	ctx->step_index = &spec->step_index;
	ctx->random_seed = spec->random_seed;
	ctx->my_bitfield = spec->my_bitfield;
	ctx->contacts_size = my_node->directions[COMM_GRAPH_FATHERS]->node_count +
			my_node->directions[COMM_GRAPH_CHILDREN]->node_count;
	ctx->contacts = calloc(ctx->contacts_size, sizeof(tree_contact_t));
	if (!ctx->contacts) {
		return ERROR;
	}

	dir_ptr = my_node->directions[COMM_GRAPH_FATHERS];
	for (idx = 0; idx < dir_ptr->node_count; idx++) {
		ctx->contacts[idx].node = dir_ptr->nodes[idx];
		ctx->contacts[idx].distance = 0;
		ctx->contacts[idx].timeout = TIMEOUT_NEVER;
		ctx->contacts[idx].his_timeout = TIMEOUT_NEVER;
	}
	for (; idx < ctx->contacts_size; idx++) {
		ctx->contacts[idx].node = dir_ptr->nodes[idx - dir_ptr->node_count];
		ctx->contacts[idx].distance = 0;
		ctx->contacts[idx].timeout = TIMEOUT_NEVER;
		ctx->contacts[idx].his_timeout = TIMEOUT_NEVER;
	}
    return OK;
}

static void tree_validate(tree_context_t *ctx)
{
    unsigned idx;
	for (idx = 0; idx < ctx->contacts_size; idx++) {
		assert((ctx->contacts[idx].timeout == TIMEOUT_NEVER) ||
			   (ctx->contacts[idx].timeout > ctx->contacts[idx].last_seen));
		assert((ctx->contacts[idx].his_timeout == TIMEOUT_NEVER) ||
			   (ctx->contacts[idx].his_timeout > ctx->contacts[idx].last_sent + ctx->latency));
	}
}

static tree_distance_t tree_pick_service_distance(tree_context_t *ctx)
{
	tree_distance_t distance;
	step_num step_index;
	float tester, rand;

	switch (ctx->service_method) {
	case TREE_SERVICE_CYCLE_RANDOM:
		tester = 1.0;
		distance = 0;
		rand = FLOAT_RANDOM(ctx);
		while (rand < tester) {
			tester /= 2.0;
			distance++;
		}
		break;

	case TREE_SERVICE_CYCLE_CALC:
		step_index = *ctx->step_index;
		distance = (pow(step_index, 2.0) * TREE_SERVICE_CYCLE_LENGTH) %
				TREE_SERVICE_CYCLE_LENGTH;
		break;
	}
	return distance;
}

static inline int tree_next_by_topology(comm_graph_t *graph,
		                                tree_context_t *ctx,
										send_item_t *result)
{
    node_id next_peer;
    unsigned order_idx;
    unsigned wait_index = ctx->next_wait_index;
    unsigned send_index = ctx->next_send_index;
    comm_graph_node_t *my_node = &graph->nodes[ctx->my_node];
    struct order tree_order[] = {
            {COMM_GRAPH_CHILDREN,       TREE_RECV},
            {COMM_GRAPH_FATHERS,        TREE_SEND},
            {COMM_GRAPH_EXTRA_FATHERS,  TREE_SEND},
            {COMM_GRAPH_MR_CHILDREN,    TREE_RECV},
            {COMM_GRAPH_EXCLUDE,        TREE_WAIT},
            {COMM_GRAPH_EXTRA_FATHERS,  TREE_RECV},
            {COMM_GRAPH_FATHERS,        TREE_RECV},
            {COMM_GRAPH_CHILDREN,       TREE_SEND},
            {COMM_GRAPH_EXTRA_CHILDREN, TREE_SEND}
    };

    for (order_idx = 0; order_idx < (sizeof(tree_order) / sizeof(*tree_order)); order_idx++) {
        enum comm_graph_direction_type dir_type = tree_order[order_idx].direction;
        comm_graph_direction_ptr_t dir_ptr = my_node->directions[dir_type];
        switch (tree_order[order_idx].action) {
        case TREE_RECV:
            if (wait_index < dir_ptr->node_count) {
                while (wait_index < dir_ptr->node_count) {
                    next_peer = dir_ptr->nodes[wait_index];
                    if (IS_BIT_SET_HERE(next_peer, ctx->my_bitfield)) {
                        wait_index++;
                    } else {
                    	ctx->next_wait_index = wait_index;
                        return OK;
                    }
                }
            }

            // TODO: wait on sons!!!

            if (wait_index >= dir_ptr->node_count) {
                wait_index -= dir_ptr->node_count;
            }
            break;

        case TREE_SEND:
            if (send_index < dir_ptr->node_count) {
                result->bitfield = BITFIELD_FILL_AND_SEND;
                result->dst = dir_ptr->nodes[send_index];
                result->msg = TREE_MSG_DATA;
                ctx->next_send_index++;
                return TREE_PACKET_CHOSEN;
            } else {
                send_index -= dir_ptr->node_count;
            }
            break;

        case TREE_WAIT:
            /* Wait for bitmap to be full before distributing */
            if (!IS_FULL_HERE(ctx->my_bitfield)) {
                return OK;
            }
            break;
        }
    }

    /* No more packets to send - we're done here! */
    return OK;
}

static inline int queue_get_msg_by_type(send_list_t *in_queue,
		                                msg_type msg,
										send_item_t *result)
{
    send_item_t *it;
    unsigned pkt_idx, used;
    for (pkt_idx = 0, it = &in_queue->items[0], used = in_queue->used;
         (pkt_idx < in_queue->allocated) && (used > 0);
         pkt_idx++, it++) {
        if (it->distance != DISTANCE_VACANT) {
            if (it->msg % TREE_MSG_MAX == msg) {
                memcpy(result, it, sizeof(*it));
                it->distance = DISTANCE_VACANT;
                in_queue->used--;
                return 1;
            }
            used--;
        }
    }
    return 0;
}

static inline int queue_get_msg_by_distance(send_list_t *in_queue,
		                                    msg_type msg,
		                                    tree_distance_t distance,
											send_item_t *result)
{
    send_item_t *it;
    unsigned pkt_idx, used;
    for (pkt_idx = 0, it = &in_queue->items[0], used = in_queue->used;
         (pkt_idx < in_queue->allocated) && (used > 0);
         pkt_idx++, it++) {
        if (it->distance != DISTANCE_VACANT) {
            if (it->msg == (msg + TREE_MSG_MAX * distance)) {
                memcpy(result, it, sizeof(*it));
                it->distance = DISTANCE_VACANT;
                in_queue->used--;
                return 1;
            }
            used--;
        }
    }
    return 0;
}

static step_num tree_calc_timeout(tree_distance_t distance, step_num latency, unsigned radix)
{
	return (pow(2, distance-1)*(pow(radix, distance)+1)/(radix+1))+(2*latency);
}

static void tree_contact_keep_alive(tree_contact_t *contact, send_item_t *result) {
	/* Update contact record */

	/* Generate output packet */
	result->bitfield = BITFIELD_FILL_AND_SEND;
	result->dst = contact->node_id;
	result->msg = TREE_MSG_KA(contact->distance);
	result->timeout =
}

int tree_next(comm_graph_t *graph, send_list_t *in_queue,
		      tree_context_t *ctx, send_item_t *result)
{
	step_num current_step_index;
	tree_distance_t distance;
	unsigned idx;
	int ret;

	/* Step #0: Assert algorithm's assumptions */
	tree_validate(ctx);

	/* Step #1: If data can be sent - send it! (w/o reading incoming messages) */
	ret = tree_next_by_topology(graph, ctx, result);
	if (ret == TREE_PACKET_CHOSEN) {
		return OK;
	}
	if (ret != OK) {
		return ret;
	}

	/* Step #2: Determine which tree-distance gets service this time */
	distance = tree_pick_service_distance(ctx);

	/* Step #3: Data (from service-distance) comes first, then keep-alives */
	if (queue_get_msg_by_distance(in_queue, TREE_MSG_DATA, distance, result) ||
		queue_get_msg_by_distance(in_queue, TREE_MSG_KEEPALIVE, distance, result) ||
		queue_get_msg_by_distance(in_queue, TREE_MSG_KEEPALIVE_ACK, distance, result)) {
		return OK;
	}

	/* Step #4: If its time is up - send a keep-alive within service-distance */
	current_step_index = *ctx->step_index;
	for (idx = 0; idx < ctx->contacts_used; idx++) {
		tree_contact_t *contact = &ctx->contacts[idx];
		if ((contact->distance == distance) &&
			(contact->timeout >= current_step_index)) {
			tree_contact_keep_alive(contact, result);
			return TREE_PACKET_CHOSEN;
		}
	}

	/* Step #5: Just read any other packet from the queue... */
	if (queue_get_msg_by_type(in_queue, TREE_MSG_DATA, result) ||
		queue_get_msg_by_type(in_queue, TREE_MSG_KEEPALIVE, result) ||
		queue_get_msg_by_type(in_queue, TREE_MSG_KEEPALIVE_ACK, result)) {
		return OK;
	}

	/* Step #6: Nothing to do - remain idle */
    result->distance = DISTANCE_NO_PACKET;
    return OK;
}

int tree_fix(comm_graph_t *graph, tree_context_t *ctx,
			 tree_recovery_method_t recovery, node_id broken)
{
    /* Exclude the broken node from further sends */
    int ret = comm_graph_append(graph, ctx->my_node, broken, COMM_GRAPH_EXCLUDE);
    if (ret != OK) {
        return ret;
    }

    printf("OMIT: %lu omits %lu\n", ctx->my_node, broken);

    switch (recovery) {
    case COLLECTIVE_RECOVERY_CATCH_THE_BUS: // TODO: implement!
    case COLLECTIVE_RECOVERY_BROTHER_FIRST: //TODO: implement!
        /* calc brother - reverse BFS */
        /* add brother as father, myself as his child */
    case COLLECTIVE_RECOVERY_FATHER_FIRST:
        if (broken < ctx->my_node) {
            /* the broken node is above me in the tree: */
            if (comm_graph_count(graph, broken, COMM_GRAPH_FATHERS) == 0) {
                /* if broken has no fathers - add his children to COMM_GRAPH_EXTRA_FATHERS and me as their son */
                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
                if (ret != OK) {
                    return ret;
                }

                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_MR_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
            } else {
                /* add broken's fathers to COMM_GRAPH_EXTRA_FATHERS, and me as their son */
                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_FATHERS, COMM_GRAPH_EXTRA_FATHERS);
            }
            if (ret != OK) {
                return ret;
            }
        }
        return comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_CHILDREN);
        break;


    case COLLECTIVE_RECOVERY_ALL:
        break;
    }
    return ERROR;
}

int tree_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * N-array tree (tree_radix=k):
     * For every level, each of the nodes of the previous level is added k
     * children.
     *
     *    Graphic example:
     *    https://ccsweb.lanl.gov/~pakin/software/conceptual/userguide/n_002dary-tree-functions.html#n_002dary-tree-functions
     *
     *
     * K-nomial tree (tree_radix=k):
     * For every level, each of the nodes so far (not just the previous
     * level) is added (k-1) children. Each node can have children on
     * multiple levels.
     *
     *    Graphic example:
     *    https://ccsweb.lanl.gov/~pakin/software/conceptual/userguide/k_002\
     *        dnomial-tree-functions.html#k_002dnomial-tree-functions
     */

    int ret;
    unsigned node_count = spec->node_count;
    unsigned tree_radix = spec->topology.tree.radix;
    int is_knomial = ((spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_TREE) ||
                      (spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE));
    int is_multiroot = ((spec->topology_type == COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE) ||
                        (spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE));

    node_id child_count;
    node_id next_child;
    node_id next_father;
    node_id first_child = 1;
    node_id first_father = 0;

    assert(tree_radix);

    *graph = comm_graph_create(node_count, 1);
    if (!*graph) {
        return ERROR;
    }

    if (is_multiroot) {
        for (next_father = 0; next_father < tree_radix; next_father++) {
            for (next_child = 0; next_child < tree_radix; next_child++) {
                if (next_father != next_child) {
                    ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_MR_CHILDREN);
                    if (ret != OK) {
                        comm_graph_destroy(*graph);
                        return ret;
                    }
                }
            }
        }
        first_child = tree_radix;
    }

    next_child = first_child;
    while (next_child < node_count) {
        for (child_count = 0; child_count < tree_radix; child_count++) {
            for (next_father = first_father;
                 (next_father < first_child) && (next_child < node_count);
                 next_father++, next_child++) {
                ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_CHILDREN);
                if (ret != OK) {
                    comm_graph_destroy(*graph);
                    return ret;
                }
            }
        }

        first_child += (first_child - first_father) * tree_radix;
        if (!is_knomial) {
            first_father = next_father;
        }
    }

    return OK;
}
