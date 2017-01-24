#include "topology.h"

struct butterfly_ctx {
    comm_graph_direction_t *my_peers;
    unsigned char *my_bitfield;
    unsigned next_child_index;
};

int butterfly_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct butterfly_ctx **internal_ctx)
{
    *internal_ctx = malloc(sizeof(struct butterfly_ctx));
    if (!*internal_ctx) {
        return ERROR;
    }

    (*internal_ctx)->next_child_index = 0;
    (*internal_ctx)->my_bitfield = spec->my_bitfield;
    (*internal_ctx)->my_peers =
            graph->nodes[spec->my_rank].directions[COMM_GRAPH_FLOW];
}

int butterfly_next(comm_graph_t *graph, struct butterfly_ctx *internal_ctx,
                   node_id *target, unsigned *distance)
{
    node_id next_partner;

    /* First send requires nothing */
    if (internal_ctx->next_child_index == 0) {
        *target = internal_ctx->my_peers->nodes[0];
        internal_ctx->next_child_index++;
        return OK;
    }

    /* Check for remaining peers */
    if (internal_ctx->next_child_index > internal_ctx->my_peers->node_count) {
        *distance = NO_PACKET;
        return OK;
    }

    /* Wait for nodes before sending on */
    next_partner =
            internal_ctx->my_peers->nodes[internal_ctx->next_child_index - 1];
    if (IS_BIT_SET_HERE(next_partner, internal_ctx->my_bitfield)) {
        *target = internal_ctx->my_peers->nodes[internal_ctx->next_child_index++];
    } else {
        *distance = NO_PACKET;
    }

    return OK;
}

int butterfly_fix(comm_graph_t *graph, void *internal_ctx, node_id broken)
{
    return ERROR;
}

int butterfly_end(struct butterfly_ctx *internal_ctx)
{
    free(internal_ctx);
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
    unsigned radix = spec->topology.tree.radix;
    node_id group_size = radix;
    node_id jump_size = 1;
    node_id next_id = 0;

    comm_graph_t* flow = comm_graph_create(node_count, COMM_GRAPH_FLOW);
    if (!flow) {
        return NULL;
    }

    if (radix > node_count) {
        radix = node_count;
    }

    while (jump_size ) {
        node_id jump;
        for (next_id = 0; next_id < node_count; next_id++) {
            for (jump = 0; jump < radix - 1; jump++) {
                node_id group_start = next_id - (next_id % group_size);
                comm_graph_append(flow, next_id,
                                  group_start + ((jump_size * jump) % group_size));
            }
        }
        jump_size = group_size;
        group_size *= radix;
    }

    return flow;
}
