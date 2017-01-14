#include "topo_map.h"

#include <assert.h>

struct topology_iterator {
	comm_graph_t *graph;
	topology_spec_t *spec;
	void *ctx; /* internal context for each iterator, type depends on topology */
	topo_funcs_t funcs;
	unsigned random_seed;
};

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;
extern topo_funcs_t topo_map[];

int topology_iterator_create(topology_spec_t *spec, topology_iterator_t *iterator)
{
	int ret_val;
	enum topology_map_slot map_slot;

	/* Select and copy the function pointers for the requested topology */
	switch (spec->topology) {
	case COLLECTIVE_TOPOLOGY_NARRAY_TREE:
	case COLLECTIVE_TOPOLOGY_KNOMIAL_TREE:
	case COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE:
	case COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE:
		map_slot = TREE;
    	break;

	case COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING:
		map_slot = BUTTERFLY;
		break;

	case COLLECTIVE_TOPOLOGY_RANDOM_PURE:
	case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST:
	case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM:
	case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR:
	case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL:
	case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC:
		map_slot = RANDOM;
		break;

	case COLLECTIVE_TOPOLOGY_ALL:
		return ERROR;
	}

	memcpy(&iterator->funcs, &topo_map[map_slot], sizeof(topo_funcs_t));

	/* Build the communication graph, if not built yet */
	if (!current_reference_count++) {
		assert(!current_topology);
		ret_val = iterator->funcs.build_f(spec, &current_topology);
		if (ret_val != OK) {
			return ret_val;
		}
	}

	iterator->spec = spec;
	iterator->graph = current_topology;
	iterator->random_seed = spec->random->random_seed;
	return iterator->funcs.start_f(spec, current_topology, &iterator->ctx);
}

int topology_iterator_next(topology_iterator_t *iterator, node_id *target, unsigned *distance)
{
    if ((iterator->spec->model == COLLECTIVE_MODEL_PACKET_DROP) &&
        (iterator->fail_rate > FLOAT_RANDOM(iterator))) {
        *distance = NO_PACKET;
        return OK;
    }

    if ((iterator->spec->model == COLLECTIVE_MODEL_TIME_OFFSET) &&
        (iterator->step_index < ctx->time_offset[ctx->my_local_rank])) {
        *distance = NO_PACKET;
        return OK;
    }

    if (iterator->spec->model == COLLECTIVE_MODEL_PACKET_DELAY) {
        distance = CYCLIC_RANDOM(iterator, iterator->delay_max);
    } else {
    	distance = 0;
    }

	return iterator->funcs.next_f(iterator->graph, iterator->ctx, target, distance);
}

int topology_iterator_omit(topology_iterator_t *iterator, node_id broken)
{
	if (iterator->graph && (iterator->graph == current_topology)) {
		iterator->graph = comm_graph_clone(current_topology);
	}

	return iterator->funcs.fix_f(iterator->graph, iterator->ctx, broken);
}

void topology_iterator_destroy(topology_iterator_t *iterator)
{
	iterator->funcs.end_f(iterator->ctx);

	if (iterator->graph && (iterator->graph != current_topology)) {
		comm_graph_destroy(iterator->graph);
	}

	if (current_topology && !--current_reference_count) {
		comm_graph_destroy(current_topology);
		current_topology = NULL;
	}
}
