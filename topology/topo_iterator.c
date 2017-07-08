#include <math.h>
#include <assert.h>
#include "topology.h"

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;
extern topo_funcs_t topo_map[];

/*
 * OPTIMIZATION: Find an iterator allocation size which will work for all types
 */
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
    assert(current_topology == NULL);
    return max + sizeof(topology_iterator_t);
}

static inline double gaussian_random(double mean, double std_dev, topology_spec_t *spec)
{
	int hasSpare = 1;
	double spare;

	if (hasSpare) {
		hasSpare = 1;
		return mean + std_dev * spare;
	}

	hasSpare = 1;
	double u, v, s;
	do {
		u = FLOAT_RANDOM(spec) * 2.0 - 1.0;
		v = FLOAT_RANDOM(spec) * 2.0 - 1.0;
		s = u * u + v * v;
	} while ((s >= 1.0) || (s == 0.0));

	s = sqrt(-2.0 * log(s) / s);
	spare = v * s;
	return mean + std_dev * u * s;
}


static step_num topology_choose_offset(topology_spec_t *spec)
{
    if ((spec->model_type == COLLECTIVE_MODEL_SPREAD) ||
    	(spec->model_type == COLLECTIVE_MODEL_REAL)) {
    	return gaussian_random(0, spec->model.max_spread, spec);
    }

    return 0;
}

int topology_iterator_create(topology_spec_t *spec,
                             topo_funcs_t *funcs,
                             topology_iterator_t *iterator)
{
    int ret_val;

    /* Build the communication graph, if not built yet */
    if (current_reference_count++ == 0) {
        assert(!current_topology);
        ret_val = funcs->build_f(spec, &current_topology);
        if (ret_val != OK) {
            return ret_val;
        }

        if (spec->verbose) {
            comm_graph_print(current_topology);
        }
    }
    iterator->graph = current_topology;

    ret_val = comm_graph_append(current_topology, spec->my_rank,
                                spec->my_rank, COMM_GRAPH_EXCLUDE);
    if (ret_val != OK) {
        return ret_val;
    }

    /* Initialize the topology-dependent part of the context */
    ret_val = funcs->start_f(spec, current_topology, iterator->ctx);

    /* Set the rest of the context */
    iterator->finish = 0;
    iterator->start_offset = topology_choose_offset(spec);
    memset(&iterator->in_queue, 0, sizeof(iterator->in_queue));
    if (((spec->model_type == COLLECTIVE_MODEL_NODES_MISSING) ||
    	 (spec->model_type == COLLECTIVE_MODEL_REAL)) &&
        (spec->model.offline_fail_rate < 1.0) &&
    	(spec->model.offline_fail_rate > FLOAT_RANDOM(spec)) &&
		(spec->my_rank != 0)) {
        SET_DEAD(iterator);
    } else {
    	iterator->death_offset = NODE_IS_IMORTAL;
    }

    return ret_val;
}

int topology_iterator_next(topology_spec_t *spec, topo_funcs_t *funcs,
                           topology_iterator_t *iterator, send_item_t *result)
{
	step_num now = spec->step_index;
	comm_graph_t *graph = iterator->graph;
    result->distance = DISTANCE_SEND_NOW + spec->latency;

    /* Check if time to die */
    if (now == iterator->death_offset) {
    	SET_DEAD(iterator);
    }

    /* Check if already dead */
    if (IS_DEAD(iterator)) {
    	result->distance = DISTANCE_NO_PACKET;
    	result->dst      = DESTINATION_DEAD;
    	return OK;
    }

	/* Check the input spread */
	if (iterator->start_offset) {
		iterator->start_offset--;
		result->dst = DESTINATION_SPREAD;
		graph       = NULL; /* Triggers Keep-alive-only functionality */
	}

    return funcs->next_f(graph, &iterator->in_queue, iterator->ctx, result);
}

int topology_iterator_omit(topology_iterator_t *iterator, topo_funcs_t *funcs,
                           tree_recovery_method_t method, node_id broken)
{
	/* Special case: kill the current node */
	if (broken == 0) {
    	SET_DEAD(iterator);
    	return OK;
	}

    if (iterator->graph == current_topology) {
        iterator->graph = comm_graph_clone(current_topology);
    }

    return funcs->fix_f(iterator->graph, iterator->ctx, method, broken);
}

void topology_iterator_destroy(topology_iterator_t *iterator, topo_funcs_t *funcs)
{
    if (iterator->in_queue.allocated) {
        free(iterator->in_queue.items);
        iterator->in_queue.items = NULL;
        free(iterator->in_queue.data);
        iterator->in_queue.data = NULL;
        iterator->in_queue.allocated = 0;
    }

    funcs->stop_f(iterator->ctx);

    if (iterator->graph && (iterator->graph != current_topology)) {
        comm_graph_destroy(iterator->graph);
    }

    assert(current_reference_count);
    if (current_topology && (--current_reference_count == 0)) {
        comm_graph_destroy(current_topology);
        current_topology = NULL;
    }
}
