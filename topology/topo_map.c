#include "topology.h"

size_t tree_ctx_size();
int tree_build(topology_spec_t *spec, comm_graph_t **graph);
int tree_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int tree_next(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
int tree_fix(comm_graph_t *graph, void *internal_ctx, tree_recovery_type_t method, node_id broken);

size_t butterfly_ctx_size();
int butterfly_build(topology_spec_t *spec, comm_graph_t **graph);
int butterfly_start(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
int butterfly_next(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
int butterfly_fix(comm_graph_t *graph, void *internal_ctx, tree_recovery_type_t method, node_id broken);

topo_funcs_t topo_map[] = {
		{
				.size_f = tree_ctx_size,
				.build_f = tree_build,
				.start_f = tree_start,
				.next_f = tree_next,
				.fix_f = tree_fix,
		},
		{
				.size_f = butterfly_ctx_size,
				.build_f = butterfly_build,
				.start_f = butterfly_start,
				.next_f = butterfly_next,
				.fix_f = butterfly_fix,
		},
};
