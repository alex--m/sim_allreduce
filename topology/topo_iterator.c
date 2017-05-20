#include <assert.h>
#include "topology.h"

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;
extern topo_funcs_t topo_map[];

size_t topology_iterator_size()
{
	size_t max = 0;
	enum topology_map_slot map_slot = 0;
	while (map_slot < MAX) {
		size_t slot_size = topo_map[map_slot++].size_f();
		if (max < slot_size) {
			max = slot_size;
		}
	}
	return max + sizeof(topology_iterator_t);
}

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

	case COLLECTIVE_TOPOLOGY_ALL:
		return ERROR;
	}
	iterator->funcs = &topo_map[map_slot];

	/* Build the communication graph, if not built yet */
	if (current_reference_count++ == 0) {
		assert(!current_topology);
		ret_val = iterator->funcs->build_f(spec, &current_topology);
		if (ret_val != OK) {
			return ret_val;
		}

		if ((spec->verbose) && (map_slot == TREE)) {
			comm_graph_print(current_topology);
		}
	}

	iterator->spec = spec;
	iterator->graph = current_topology;
	iterator->random_seed = spec->random_seed;
	iterator->time_offset = (iterator->spec->model_type == COLLECTIVE_MODEL_TIME_OFFSET) ?
			CYCLIC_RANDOM(iterator->spec, iterator->spec->model.time_offset_max) : 0;
	if ((iterator->spec->model_type == COLLECTIVE_MODEL_NODES_MISSING) &&
		(iterator->spec->model.node_fail_rate > FLOAT_RANDOM(iterator->spec))) {
		SET_DEAD(iterator);
	}

	ret_val = comm_graph_append(current_topology, spec->my_rank, spec->my_rank, COMM_GRAPH_EXCLUDE);
	if (ret_val != OK) {
		return ret_val;
	}

	return iterator->funcs->start_f(spec, current_topology, &iterator->ctx[0]);
}

int topology_iterator_next(topology_iterator_t *iterator, node_id *target, unsigned *distance)
{
	*distance = 0;
	switch (iterator->spec->model_type)
	{
	case COLLECTIVE_MODEL_FIXED_DELAY:
        *distance = iterator->spec->model.packet_delay;
        break;

	case COLLECTIVE_MODEL_RANDOM_DELAY:
        *distance = CYCLIC_RANDOM(iterator->spec, iterator->spec->model.packet_delay);
        break;

	case COLLECTIVE_MODEL_PACKET_DROP:
		if (iterator->spec->model.packet_drop_rate > FLOAT_RANDOM(iterator->spec)) {
			printf("\nPacket DROPPED!\n");
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

	case COLLECTIVE_MODEL_NODES_FAILING:
		if (iterator->spec->model.node_fail_rate > FLOAT_RANDOM(iterator->spec)) {
			SET_DEAD(iterator);
		}
		/* Intentionally no break */

	case COLLECTIVE_MODEL_NODES_MISSING:
		if (IS_DEAD(iterator)) {
			*distance = iterator->spec->death_timeout; /* Dead are "sending" their timeout */
		}
		break;

	default:
		break;
    }

	return iterator->funcs->next_f(iterator->graph, iterator->ctx, target, distance);
}

int topology_iterator_omit(topology_iterator_t *iterator, tree_recovery_type_t method, node_id broken)
{
	if (iterator->graph && (iterator->graph == current_topology)) {
		iterator->graph = comm_graph_clone(current_topology);
	}

	return iterator->funcs->fix_f(iterator->graph, iterator->ctx, method, broken);
}

void topology_iterator_destroy(topology_iterator_t *iterator)
{
	if (iterator->graph && (iterator->graph != current_topology)) {
		comm_graph_destroy(iterator->graph);
	}

	assert(current_reference_count);
	if (current_topology && (--current_reference_count == 0)) {
		comm_graph_destroy(current_topology);
		current_topology = NULL;
	}
}
