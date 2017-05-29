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
    return max + sizeof(topology_iterator_t);
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
    iterator->time_offset =
            (spec->model_type == COLLECTIVE_MODEL_SPREAD) ?
            CYCLIC_RANDOM(spec, spec->model.max_spread) : 0;
    memset(&iterator->in_queue, 0, sizeof(iterator->in_queue));
    if ((spec->model_type == COLLECTIVE_MODEL_NODES_MISSING) &&
        (spec->model.node_fail_rate > FLOAT_RANDOM(spec))) {
        SET_DEAD(iterator);
    }

    ret_val = comm_graph_append(current_topology, spec->my_rank,
                                spec->my_rank, COMM_GRAPH_EXCLUDE);
    if (ret_val != OK) {
        return ret_val;
    }

    return funcs->start_f(spec, current_topology, &iterator->ctx[0]);
}

int topology_iterator_next(topology_spec_t *spec, topo_funcs_t *funcs,
                           topology_iterator_t *iterator, send_item_t *result)
{
    if (iterator->time_finished) {
        return DONE;
    }

    result->distance = DISTANCE_SEND_NOW + spec->latency;
    switch (spec->model_type)
    {
    case COLLECTIVE_MODEL_SPREAD: // TODO: problem: need to answer KAs during spread!
        if (iterator->time_offset) {
            iterator->time_offset--;
            result->distance = DISTANCE_NO_PACKET;
            result->dst = DESTINATION_SPREAD;
            return OK;
        }
        break;

    case COLLECTIVE_MODEL_NODES_FAILING:
        if (spec->model.node_fail_rate > FLOAT_RANDOM(spec)) {
            SET_DEAD(iterator);
        }
        /* Intentionally no break */

    case COLLECTIVE_MODEL_NODES_MISSING:
        if (IS_DEAD(iterator)) {
            result->distance = DISTANCE_NO_PACKET;
            result->dst = DESTINATION_DEAD;
            return OK;
        }
        break;

    default:
        break;
    }

    return funcs->next_f(iterator->graph, &iterator->in_queue, iterator->ctx, result);
}

int topology_iterator_omit(topology_iterator_t *iterator, topo_funcs_t *funcs,
                           tree_recovery_type_t method, node_id broken)
{
    if (iterator->graph == current_topology) {
        iterator->graph = comm_graph_clone(current_topology);
    }

    return funcs->fix_f(iterator->graph, iterator->ctx, method, broken);
}

void topology_iterator_destroy(topology_iterator_t *iterator)
{
    if (iterator->in_queue.allocated) {
        free(iterator->in_queue.items);
        iterator->in_queue.items = NULL;
        free(iterator->in_queue.data);
        iterator->in_queue.data = NULL;
    }

    if (iterator->graph && (iterator->graph != current_topology)) {
        comm_graph_destroy(iterator->graph);
    }

    assert(current_reference_count);
    if (current_topology && (--current_reference_count == 0)) {
        comm_graph_destroy(current_topology);
        current_topology = NULL;
    }
}
