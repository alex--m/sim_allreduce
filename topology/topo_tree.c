#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

struct tree_ctx {
    comm_graph_direction_t *my_peers_down;
    comm_graph_direction_t *my_peers_up;
    unsigned char *my_bitfield;
    unsigned next_wait_index;
    unsigned next_send_index;
    int no_wait_for_sons;
};

int tree_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct tree_ctx **internal_ctx)
{
	int is_multiroot = ((spec->topology_type == COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE) ||
						(spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE));


    *internal_ctx = malloc(sizeof(struct tree_ctx));
    if (!*internal_ctx) {
        return ERROR;
    }


    (*internal_ctx)->no_wait_for_sons = is_multiroot &&
    		spec->my_rank < spec->topology.tree.radix;
    (*internal_ctx)->next_wait_index = 0;
    (*internal_ctx)->next_send_index = 0;
    (*internal_ctx)->my_bitfield = spec->my_bitfield;
    (*internal_ctx)->my_peers_up =
            graph->nodes[spec->my_rank].directions[1];
    (*internal_ctx)->my_peers_down =
            graph->nodes[spec->my_rank].directions[0];
    return OK;
}

int tree_next(comm_graph_t *graph, struct tree_ctx *internal_ctx,
                   node_id *target, unsigned *distance)
{
    node_id next_peer;

    if (!internal_ctx->no_wait_for_sons) {
    	/* Wait for nodes going down */
    	while (internal_ctx->next_wait_index < internal_ctx->my_peers_down->node_count) {
    		next_peer = internal_ctx->my_peers_down->nodes[internal_ctx->next_wait_index];
    		if (IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
    			internal_ctx->next_wait_index++;
    		} else {
    			*distance = NO_PACKET;
    			return OK;
    		}
    	}
    }

    /* Send all nodes going up */
    while (internal_ctx->next_send_index < internal_ctx->my_peers_up->node_count) {
    	*target = internal_ctx->my_peers_up->nodes[internal_ctx->next_send_index++];
    	return OK;
    }

    /* Wait for nodes going up */
    while (internal_ctx->next_wait_index < internal_ctx->my_peers_down->node_count + internal_ctx->my_peers_up->node_count) {
    	next_peer = internal_ctx->my_peers_up->nodes[internal_ctx->next_wait_index - internal_ctx->my_peers_down->node_count];
    	if (IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
    			internal_ctx->next_wait_index++;
    	    } else {
    	        *distance = NO_PACKET;
    	        return OK;
    	    }
    }

    /* Send all nodes going down */
    while (internal_ctx->next_send_index < internal_ctx->my_peers_up->node_count + internal_ctx->my_peers_down->node_count) {
    	*target = internal_ctx->my_peers_down->nodes[internal_ctx->next_send_index++ - internal_ctx->my_peers_up->node_count];
    	return OK;
    }

    /* No more packets to send - draw blanks... */
    *distance = NO_PACKET;
    return OK;
}

int tree_fix(comm_graph_t *graph, void *internal_ctx, node_id broken)
{
    return ERROR;
}

int tree_end(struct tree_ctx *internal_ctx)
{
    free(internal_ctx);
    return OK;
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

	*graph = comm_graph_create(node_count, COMM_GRAPH_BIDI);
	if (!*graph) {
		return ERROR;
	}

	if (is_multiroot) {
		for (next_father = 0; next_father < tree_radix; next_father++) {
			for (next_child = 0; next_child < tree_radix; next_child++) {
				if (next_father != next_child) {
					ret = comm_graph_append(*graph, next_father, next_child);
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
				ret = comm_graph_append(*graph, next_father, next_child);
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
