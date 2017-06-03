#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

#define TREE_PACKET_CHOSEN (1)

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
    node_id my_node;
    step_num latency;
    unsigned seed;

    unsigned next_wait_index;
    unsigned next_send_index;

    unsigned contacts_size;
    unsigned contacts_used;
    tree_contact_t *contacts;
} tree_context_t;


enum tree_msg_type {
    TREE_MSG_DATA          = 0,
    TREE_MSG_KEEPALIVE     = 1,
#define TREE_MSG_KA(distance)  (TREE_MSG_KEEPALIVE     + 2*distance)
    TREE_MSG_KEEPALIVE_ACK = 2,
#define TREE_MSG_ACK(distance) (TREE_MSG_KEEPALIVE_ACK + 2*distance)
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
    return sizeof(struct tree_ctx);
}

static step_num tree_calc_timeout(tree_distance_t distance, step_num latency, unsigned radix)
{
	return (pow(2, distance-1)*(pow(radix, distance)+1)/(radix+1))+(2*latency);
}

int tree_start(topology_spec_t *spec, comm_graph_t *graph, tree_context_t *ctx)
{
    comm_graph_node_t *my_node = &graph->nodes[spec->my_rank];
    comm_graph_direction_ptr_t dir_ptr;
    unsigned idx;

	memset(ctx, 0, sizeof(*ctx));
	ctx->my_node = spec->my_rank;
	ctx->latency = spec->latency;
	ctx->seed = spec->random_seed;
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

static inline int tree_next_by_topology(comm_graph_t *graph,
		tree_context_t *ctx, send_item_t *result)
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

static inline int tree_next_by_service_cycle(send_list_t *in_queue,
		tree_context_t *ctx, send_item_t *result)
{
	unsigned idx, max_timeout_idx;

	/* Determine which tree-distance gets service this time */
	float tester = 1.0, rand = FLOAT_RANDOM(ctx->seed);
	tree_distance_t distance = 0;
	while (rand < tester) {
		tester /= 2.0;
		distance++;
	}

	if (queue_check_msg(in_queue, TREE_MSG_ACK(distance), result)) {
		return OK;
	}

	if (queue_check_msg(in_queue, TREE_MSG_KA(distance), result)) {
		return OK;
	}

	for (idx = 0; idx < ctx->contacts_used; idx++) {
		/* Choose the least recent contact with the given distance - KA it */


		return TREE_PACKET_CHOSEN;
	}

	return OK;
}

int tree_next(comm_graph_t *graph, send_list_t *in_queue,
              struct tree_ctx *internal_ctx, send_item_t *result)
{
	/* Step #0: Assert algorithm's assumptions */
	tree_validate(ctx);

	/* Step #1: Data is first priority - recv/send data if possible */
	if (ctx->is_blocking) {
		/* Check for incoming data packets */
		if (queue_check_msg(in_queue, TREE_MSG_DATA, result)) {
			return OK;
		}

		/* Check for outgoing data packets */
		if (tree_next_by_topology(ctx)) {
			return ctx->is_done ? DONE : OK;
		}
	}

	/* Step #2: Serve peers by "service-cycle" */
	if (tree_next_by_service_cycle(in_queue, ctx, result)) {
		return OK;
	}

	/* Step #3: Just read any incoming packet */
	// TODO: read from the queue...

    result->distance = DISTANCE_NO_PACKET;
    return OK;
}

int tree_fix(comm_graph_t *graph, tree_context_t *ctx,
             tree_recovery_type_t method, node_id broken)
{
    /* Exclude the broken node from further sends */
    int ret = comm_graph_append(graph, internal_ctx->my_node, broken, COMM_GRAPH_EXCLUDE);
    if (ret != OK) {
        return ret;
    }

    printf("OMIT: %lu omits %lu\n", internal_ctx->my_node, broken);

    switch (method) {
    case COLLECTIVE_RECOVERY_CATCH_THE_BUS: // TODO: implement!
    case COLLECTIVE_RECOVERY_BROTHER_FIRST: //TODO: implement!
        /* calc brother - reverse BFS */
        /* add brother as father, myself as his child */
    case COLLECTIVE_RECOVERY_FATHER_FIRST:
        if (broken < internal_ctx->my_node) {
            /* the broken node is above me in the tree: */
            if (comm_graph_count(graph, broken, COMM_GRAPH_FATHERS) == 0) {
                /* if broken has no fathers - add his children to COMM_GRAPH_EXTRA_FATHERS and me as their son */
                ret = comm_graph_copy(graph, broken, internal_ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
                if (ret != OK) {
                    return ret;
                }

                ret = comm_graph_copy(graph, broken, internal_ctx->my_node, COMM_GRAPH_MR_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
            } else {
                /* add broken's fathers to COMM_GRAPH_EXTRA_FATHERS, and me as their son */
                ret = comm_graph_copy(graph, broken, internal_ctx->my_node, COMM_GRAPH_FATHERS, COMM_GRAPH_EXTRA_FATHERS);
            }
            if (ret != OK) {
                return ret;
            }
        }
        return comm_graph_copy(graph, broken, internal_ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_CHILDREN);
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
