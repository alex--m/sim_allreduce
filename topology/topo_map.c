#include "topology.h"

int tree_build(topology_spec_t *spec, comm_graph_t **graph);
int tree_start(topology_spec_t *spec, comm_graph_t *graph, void **internal_ctx);
int tree_next(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
int tree_fix(comm_graph_t *graph, void *internal_ctx, tree_recovery_type_t method, node_id broken);
int tree_end(void *internal_ctx);

int butterfly_build(topology_spec_t *spec, comm_graph_t **graph);
int butterfly_start(topology_spec_t *spec, comm_graph_t *graph, void **internal_ctx);
int butterfly_next(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
int butterfly_fix(comm_graph_t *graph, void *internal_ctx, tree_recovery_type_t method, node_id broken);
int butterfly_end(void *internal_ctx);

topo_funcs_t topo_map[] = {
		{
				.build_f = tree_build,
				.start_f = tree_start,
				.next_f = tree_next,
				.fix_f = tree_fix,
				.end_f = tree_end,
		},
		{
				.build_f = butterfly_build,
				.start_f = butterfly_start,
				.next_f = butterfly_next,
				.fix_f = butterfly_fix,
				.end_f = butterfly_end,
		},
};
