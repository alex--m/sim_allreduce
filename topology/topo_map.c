#include "topology.h"

size_t tree_ctx_size();
int tree_build(topology_spec_t *spec, comm_graph_t **graph);
int tree_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int tree_next(comm_graph_t *graph, send_list_t *in_queue,
              void *internal_ctx, send_item_t *result);
int tree_fix(comm_graph_t *graph, void *internal_ctx,
             tree_recovery_method_t recovery, node_id broken);
void tree_stop(void *internal_ctx);

size_t butterfly_ctx_size();
int butterfly_build(topology_spec_t *spec, comm_graph_t **graph);
int butterfly_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int butterfly_next(comm_graph_t *graph, send_list_t *in_queue,
                   void *internal_ctx, send_item_t *result);
int butterfly_fix(comm_graph_t *graph, void *internal_ctx,
		          tree_recovery_method_t recovery, node_id broken);
void butterfly_stop(void *internal_ctx);

topo_funcs_t topo_map[] = {
        {
                .size_f = tree_ctx_size,
                .build_f = tree_build,
                .start_f = tree_start,
                .next_f = tree_next,
                .fix_f = tree_fix,
                .stop_f = tree_stop,
        },
        {
                .size_f = butterfly_ctx_size,
                .build_f = butterfly_build,
                .start_f = butterfly_start,
                .next_f = butterfly_next,
                .fix_f = butterfly_fix,
                .stop_f = butterfly_stop,
        },
};
