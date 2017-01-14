#include "topology.h"

enum topology_map_slot
{
	TREE,
	BUTTERFLY,
	RANDOM
};

typedef struct topo_funcs
{
	int (*build_f)(topology_spec_t *spec, comm_graph_t **graph);
	int (*start_f)(topology_spec_t *spec, comm_graph_t *graph, void **internal_ctx);
	int (*next_f)(void *internal_ctx, node_id *target, unsigned *distance);
	int (*fix_f)();
	int (*end_f)(void *internal_ctx);
} topo_funcs_t;
