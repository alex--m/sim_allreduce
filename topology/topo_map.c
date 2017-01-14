#include "topo_map.h"

int tree_build(topology_spec_t spec, comm_graph_t **graph);
int tree_start(topology_spec_t spec, comm_graph_t *graph, void **internal_ctx);
int tree_next(void *internal_ctx, node_id *target, unsigned *distance);
int tree_fix();
int tree_end(void *internal_ctx);

int butterfly_build(topology_spec_t spec, comm_graph_t **graph);
int butterfly_start(topology_spec_t spec, comm_graph_t *graph, void **internal_ctx);
int butterfly_next(void *internal_ctx, node_id *target, unsigned *distance);
int butterfly_fix();
int butterfly_end(void *internal_ctx);

int random_build(topology_spec_t spec, comm_graph_t **graph);
int random_start(topology_spec_t spec, comm_graph_t *graph, void **internal_ctx);
int random_next(void *internal_ctx, node_id *target, unsigned *distance);
int random_fix();
int random_end(void *internal_ctx);

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
		{
				.build_f = random_build,
				.start_f = random_start,
				.next_f = random_next,
				.fix_f = random_fix,
				.end_f = random_end,
		},
};
