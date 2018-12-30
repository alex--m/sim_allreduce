#include "../state/state_matrix.h"
#include "topology.h"

struct optimal_ctx {
    comm_graph_direction_t *my_peers;
    unsigned char *my_bitfield;
    unsigned next_index;
    node_id my_rank;
};

size_t optimal_ctx_size() {
    return sizeof(struct optimal_ctx);
}

int optimal_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct optimal_ctx *internal_ctx)
{
    internal_ctx->next_index  = 0;
    internal_ctx->my_bitfield = spec->my_bitfield;
    internal_ctx->my_peers    = graph->nodes[spec->my_rank].directions[COMM_GRAPH_CHILDREN];
    internal_ctx->my_rank     = spec->my_rank;
    return OK;
}

void optimal_stop(struct optimal_ctx *internal_ctx)
{
	free(internal_ctx);
}

int optimal_next(comm_graph_t *graph, send_list_t *in_queue,
                    struct optimal_ctx *internal_ctx, send_item_t *result)
{
    return OK;
}

int optimal_fix(comm_graph_t *graph, void *internal_ctx,
		          tree_recovery_method_t recovery, node_id broken)
{
    return OK;
}

int optimal_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * Optimal tree... TODO
     *
     * Graphic example :
     * <TBD>
     */

    return OK;
}

