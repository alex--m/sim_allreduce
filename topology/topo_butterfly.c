#include "topology.h"
#include "../state/state_matrix.h"

struct butterfly_ctx {
    comm_graph_direction_t *my_peers;
    unsigned char *my_bitfield;
    unsigned next_child_index;
    unsigned check_interval;

    node_id first_extra; /* if non power of radix */
    node_id node_count;
    unsigned power;
};

static inline unsigned get_closest_power(node_id node_count, unsigned radix)
{
	unsigned power = 1;
	while (power * radix <= node_count) {
		power *= radix;
	}
	return power;
}

size_t butterfly_ctx_size() {
	return sizeof(struct butterfly_ctx);
}

int butterfly_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct butterfly_ctx *internal_ctx)
{
    unsigned radix = spec->topology.butterfly.radix;
	unsigned power = get_closest_power(spec->node_count, radix);

    internal_ctx->check_interval = radix - 1;
    internal_ctx->next_child_index = 0;
    internal_ctx->my_bitfield = spec->my_bitfield;
    internal_ctx->node_count = spec->node_count;
    internal_ctx->my_peers = graph->nodes[spec->my_rank].directions[COMM_GRAPH_CHILDREN];
    internal_ctx->first_extra = ((spec->node_count != power) &&
    		(spec->my_rank < power)) ? (spec->my_rank + power) : 0;
    internal_ctx->power = power;
    return OK;
}

int butterfly_next(comm_graph_t *graph, struct butterfly_ctx *internal_ctx,
                   node_id *target, unsigned *distance)
{
    node_id idx, next_peer;

    /* Check for remaining peers */
    if (internal_ctx->next_child_index >= internal_ctx->my_peers->node_count) {
        return DONE;
    }

    /* Wait for excess nodes */
    if (internal_ctx->first_extra) {
    	for (next_peer = internal_ctx->first_extra;
    		 next_peer < internal_ctx->node_count;
    		 next_peer += internal_ctx->power)
		if (!IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
			*distance = NO_PACKET;
			*target = next_peer;
			return OK;
		}
		internal_ctx->first_extra = 0;
    }

    /* Wait for this level before ascending to the next level */
    if (internal_ctx->next_child_index % internal_ctx->check_interval == 0) {
    	next_peer = (internal_ctx->next_child_index == 0) ? 0 :
    			(internal_ctx->next_child_index - internal_ctx->check_interval);
    	for (idx = next_peer; idx < internal_ctx->next_child_index; idx++) {
    		next_peer = internal_ctx->my_peers->nodes[idx];
    		if (!IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
    	        *distance = NO_PACKET;
    			*target = next_peer;
    	        return OK;
    		}
    	}
    }

    /* Send to the next target on the list */
    *target = internal_ctx->my_peers->nodes[internal_ctx->next_child_index++];
    return OK;
}

int butterfly_fix(comm_graph_t *graph, void *internal_ctx, node_id broken)
{
    return ERROR;
}

int butterfly_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * Recursive K-ing (tree_radix=k):
     * For every level X - divide the ranks into groups of size k^(X-1),
     * assign each rank with the its relative index in that group, the divide
     * the ranks into groups of size k^X, and send to all members of the new
     * group assigned the same index as you.
     * If the last group (of size Y) is incomplete (Y < X): senders within
     * this group of relative index exceeding Y will send to the index modulo Y.
     *
     * Graphic example :
     * http://www.mcs.anl.gov/~thakur/papers/ijhpca-coll.pdf (for recursive doubling)
     * http://dl.acm.org/citation.cfm?id=2402483 (Reindexed Recursive K-ing)
     */

    unsigned node_count = spec->node_count;
    unsigned radix = spec->topology.butterfly.radix;
    node_id max_group_size;
    node_id group_size;
    node_id jump_size;
    node_id next_id;
    node_id jump;

    *graph = comm_graph_create(node_count, 0);
    if (!*graph) {
        return ERROR;
    }

    /* Set excess nodes (non power of radix) to check in */
    max_group_size = get_closest_power(node_count, radix);
    for (next_id = max_group_size; next_id < node_count; next_id++) {
    	comm_graph_append(*graph, next_id, next_id % max_group_size,
    			COMM_GRAPH_CHILDREN);
    }

    for (jump_size = 1, group_size = radix;
    	 jump_size < max_group_size;
    	 jump_size = group_size, group_size *= radix) {
        for (next_id = 0; next_id < max_group_size; next_id++) {
            for (jump = 1; jump < radix; jump++) {
                node_id group_start = next_id - (next_id % group_size);
                comm_graph_append(*graph, next_id, group_start +
                		          (((next_id - group_start) +
                				  (jump_size * jump)) % group_size),
								  COMM_GRAPH_CHILDREN);
            }
        }
    }

    /* Provide excess nodes (non power of radix) with the result */
    for (next_id = max_group_size; next_id < node_count; next_id++) {
    	comm_graph_append(*graph, next_id % max_group_size, next_id,
    			COMM_GRAPH_CHILDREN);
    }

    return OK;
}
