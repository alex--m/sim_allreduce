#include <assert.h>
#include "topology.h"

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;
extern topo_funcs_t topo_map[];

int topology_iterator_create(topology_spec_t *spec, topology_iterator_t *iterator)
{
	int ret_val;
	enum topology_map_slot map_slot;

	/* Select and copy the function pointers for the requested topology */
	switch (spec->topology_type) {
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
	if (current_reference_count++ == 0) {
		assert(!current_topology);
		ret_val = iterator->funcs.build_f(spec, &current_topology);
		if (ret_val != OK) {
			return ret_val;
		}

		if ((spec->verbose) && (map_slot == TREE)) {
			comm_graph_print(current_topology);
		}
	}

	iterator->spec = spec;
	iterator->graph = current_topology;
	iterator->random_seed = spec->topology.random.random_seed;
	iterator->time_offset = (iterator->spec->model_type == COLLECTIVE_MODEL_TIME_OFFSET) ?
			CYCLIC_RANDOM(iterator->spec, iterator->spec->model.time_offset_max) : 0;
	return iterator->funcs.start_f(spec, current_topology, &iterator->ctx);
}

int topology_iterator_next(topology_iterator_t *iterator, node_id *target, unsigned *distance)
{
	*distance = 0;
	switch (iterator->spec->model_type)
	{
	case COLLECTIVE_MODEL_PACKET_DELAY:
        *distance = CYCLIC_RANDOM(iterator->spec, iterator->spec->model.packet_delay_max);
        break;

	case COLLECTIVE_MODEL_PACKET_DROP:
		if (iterator->spec->model.packet_drop_rate > FLOAT_RANDOM(iterator->spec)) {
			*distance = NO_PACKET;
			return OK;
		}
		break;

	case COLLECTIVE_MODEL_TIME_OFFSET:
        if (iterator->time_offset) {
        	iterator->time_offset--;
			*distance = NO_PACKET;
			return OK;
        }
        break;

	default:
		break;
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

	assert(current_reference_count);
	if (current_topology && (--current_reference_count == 0)) {
		comm_graph_destroy(current_topology);
		current_topology = NULL;
	}
}
