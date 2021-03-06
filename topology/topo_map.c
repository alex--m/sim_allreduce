#include "topology.h"

size_t tree_ctx_size();
int tree_build(topology_spec_t *spec, comm_graph_t **graph);
int tree_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int tree_next(comm_graph_t *graph, send_list_t *in_queue,
              void *internal_ctx, send_item_t *result);
int tree_fix(comm_graph_t *graph, void *internal_ctx,
             tree_recovery_method_t recovery, node_id source,
             int source_is_dead);
void tree_stop(void *internal_ctx);


size_t optimal_ctx_size();
int optimal_build(topology_spec_t *spec, comm_graph_t **graph);
int optimal_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int optimal_next(comm_graph_t *graph, send_list_t *in_queue,
              void *internal_ctx, send_item_t *result);
int optimal_fix(comm_graph_t *graph, void *internal_ctx,
             tree_recovery_method_t recovery, node_id source,
             int source_is_dead);
void optimal_stop(void *internal_ctx);


size_t redundancy_ctx_size();
int debruijn_build(topology_spec_t *spec, comm_graph_t **graph);
int hypercube_build(topology_spec_t *spec, comm_graph_t **graph);
int redundancy_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int redundancy_next(comm_graph_t *graph, send_list_t *in_queue,
                   void *internal_ctx, send_item_t *result);
int redundancy_fix(comm_graph_t *graph, void *internal_ctx,
                  tree_recovery_method_t recovery, node_id source,
                  int source_is_dead);
void redundancy_stop(void *internal_ctx);


size_t butterfly_ctx_size();
int butterfly_build(topology_spec_t *spec, comm_graph_t **graph);
int butterfly_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int butterfly_next(comm_graph_t *graph, send_list_t *in_queue,
                   void *internal_ctx, send_item_t *result);
int butterfly_fix(comm_graph_t *graph, void *internal_ctx,
                  tree_recovery_method_t recovery, node_id source,
                  int source_is_dead);
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
                .size_f = optimal_ctx_size,
                .build_f = optimal_build,
                .start_f = optimal_start,
                .next_f = optimal_next,
                .fix_f = optimal_fix,
                .stop_f = optimal_stop,
        },
        {
                .size_f = redundancy_ctx_size,
                .build_f = debruijn_build,
                .start_f = redundancy_start,
                .next_f = redundancy_next,
                .fix_f = redundancy_fix,
                .stop_f = redundancy_stop,
        },
        {
                .size_f = redundancy_ctx_size,
                .build_f = hypercube_build,
                .start_f = redundancy_start,
                .next_f = redundancy_next,
                .fix_f = redundancy_fix,
                .stop_f = redundancy_stop,
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
