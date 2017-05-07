#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

struct tree_ctx {
	node_id my_node;
    comm_graph_direction_t *my_peers_down;
    comm_graph_direction_t *my_peers_up;
    unsigned char *my_bitfield;
    unsigned next_wait_index;
    unsigned next_send_index;
};

enum tree_ation {
	TREE_SEND,
	TREE_RECV,
	TREE_WAIT
};

struct order {
	enum comm_graph_direction_type direction;
	enum tree_ation action;
};

size_t tree_ctx_size() {
	return sizeof(struct tree_ctx);
}

int tree_start(topology_spec_t *spec, comm_graph_t *graph,
               struct tree_ctx *internal_ctx)
{
    internal_ctx->next_wait_index = 0;
    internal_ctx->next_send_index = 0;
    internal_ctx->my_bitfield = spec->my_bitfield;
    assert(graph->node_count > spec->my_rank);
    internal_ctx->my_peers_up =
            graph->nodes[spec->my_rank].directions[COMM_GRAPH_FATHERS];
    internal_ctx->my_peers_down =
            graph->nodes[spec->my_rank].directions[COMM_GRAPH_CHILDREN];
    internal_ctx->my_node = spec->my_rank;
    return OK;
}

int tree_next(comm_graph_t *graph, struct tree_ctx *internal_ctx,
              node_id *target, unsigned *distance)
{
    node_id next_peer;
	unsigned order_idx;
	unsigned wait_index = internal_ctx->next_wait_index;
	unsigned send_index = internal_ctx->next_send_index;
    struct order tree_order[] = {
    		{COMM_GRAPH_CHILDREN,       TREE_RECV},
			{COMM_GRAPH_FATHERS,        TREE_SEND},
			{COMM_GRAPH_EXTRA_FATHERS,  TREE_SEND},
			{COMM_GRAPH_EXCLUDE,        TREE_WAIT},
			{COMM_GRAPH_EXTRA_FATHERS,  TREE_RECV},
			{COMM_GRAPH_FATHERS,        TREE_RECV},
    		{COMM_GRAPH_EXTRA_CHILDREN, TREE_SEND}
    };

    for (order_idx = 0; order_idx < (sizeof(tree_order) / sizeof(*tree_order)); order_idx++) {
    	enum comm_graph_direction_type dir_type = tree_order[order_idx].direction;
    	comm_graph_direction_ptr_t dir_ptr =
    			graph->nodes[internal_ctx->my_node].directions[dir_type];
    	switch (tree_order[order_idx].action) {
    	case TREE_RECV:
    		if (wait_index < dir_ptr->node_count) {
    			while (wait_index < dir_ptr->node_count) {
    				next_peer = dir_ptr->nodes[wait_index];
    				if (IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
    					internal_ctx->next_wait_index++;
    					wait_index++;
    				} else {
    					*distance = NO_PACKET;
    					*target = next_peer;
    					return OK;
    				}
    			}
    		} else {
    			wait_index -= dir_ptr->node_count;
    		}
    		break;

    	case TREE_SEND:
    		if (send_index < dir_ptr->node_count) {
    			*target = dir_ptr->nodes[send_index];
    			internal_ctx->next_send_index++;
    			return OK;
    		} else {
    			send_index -= dir_ptr->node_count;
    		}
    		break;

    	case TREE_WAIT:
    		/* Wait for bitmap to be full before distributing */
    		if (!IS_FULL_HERE(internal_ctx->my_bitfield))
    		{
    			*distance = NO_PACKET;
    			*target = -1;
    			return OK;
    		}
    		break;
    	}
    }

    /* No more packets to send - we're done here! */
    return DONE;
}

int tree_fix(comm_graph_t *graph, struct tree_ctx *internal_ctx,
			 tree_recovery_type_t method, node_id broken)
{
	/* Exclude the broken node from further sends */
	comm_graph_append(graph, internal_ctx->my_node, broken, COMM_GRAPH_EXCLUDE);

	switch (method) {
	case COLLECTIVE_RECOVERY_BROTHER_FIRST: //TODO: implement!
		/* calc brother - reverse BFS */
		/* add brother as father, myself as his child */
	case COLLECTIVE_RECOVERY_FATHER_FIRST:
		if (broken < internal_ctx->my_node) {
			/* the broken node is above me in the tree: */
			/* add broken's fathers to COMM_GRAPH_EXTRA_FATHERS, and me as their son */
			enum comm_graph_direction_type adopted = COMM_GRAPH_FATHERS;

			/* if broken has no fathers - add his children to COMM_GRAPH_EXTRA_FATHERS and me as their son */
			if (comm_graph_count(graph, broken, COMM_GRAPH_FATHERS) == 0) { // TODO: what if it's not located on the same node?
				adopted = COMM_GRAPH_CHILDREN;
			}

			return comm_graph_copy(graph, broken, internal_ctx->my_node, adopted, COMM_GRAPH_EXTRA_FATHERS);
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
					ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_CHILDREN);
					if (ret != OK) {
						comm_graph_destroy(*graph);
						return ret;
					}
				}
			}
		}
		for (next_father = 0; next_father < tree_radix; next_father++) {
			(*graph)->nodes[next_father].directions[0]->node_count = 0;
			/* GRAPH API HACK... for correctness of multi-root trees */
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
