#include <math.h>
#include <assert.h>
#include "topology.h"

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;
extern topo_funcs_t topo_map[];
step_num topology_max_offset = 0;

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

double NormalCDFInverse(double p);
double RationalApproximation(double t);
double RationalApproximation(double t)
{
    double c[] = {2.515517, 0.802853, 0.010328};
    double d[] = {1.432788, 0.189269, 0.001308};
    return (t - ((c[2]*t + c[1])*t + c[0]) /
                   (((d[2]*t + d[1])*t + d[0])*t + 1.0));
}
double NormalCDFInverse(double p)
{
    assert(p > 0.0 && p < 1.0);
    return (p < 0.5)?
        -RationalApproximation(sqrt(-2.0 * log(p))):
        RationalApproximation(sqrt(-2.0 * log(1 - p)));
}

double icdf(double p, double av, double sd)
{
    return(NormalCDFInverse(p) * sd + av);
}

long gaussian_random(long spread, topology_spec_t *spec)
{
    static long base = -1;
    static long savedspread;

    if(base == -1 || spread != savedspread)
    {
        base = -icdf(1.0 / (RAND_MAX + 2.0), 0.0, spread);
        savedspread = spread;
    }
    // TODO: use FLOAT_RANDOM(spec)!
    return(base + icdf((rand() + 1.0) / (RAND_MAX + 2.0), 0.0, (double)spread));
}

static step_num topology_choose_offset(topology_spec_t *spec)
{
    if ((spec->model_type == COLLECTIVE_MODEL_SPREAD) ||
        (spec->model_type == COLLECTIVE_MODEL_REAL)) {
        switch (spec->model.spread_mode){
        case SPREAD_DISTRIBUTION_UNIFORM:
            return CYCLIC_RANDOM(spec, spec->model.spread_avg);
        case SPREAD_DISTRIBUTION_NORMAL:
            return gaussian_random(spec->model.spread_avg / 10, spec);
        }
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

        if ((spec->verbose) && (!spec->async_mode)) {
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
    if (topology_max_offset < iterator->start_offset) {
        topology_max_offset = iterator->start_offset;
    }

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

int topology_iterator_next(topology_spec_t *spec,
                           topo_funcs_t *funcs,
                           topology_iterator_t *iterator,
                           send_list_t *global_queue,
                           send_item_t *result)
{
    int idx, ret;
    step_num now = spec->step_index;
    comm_graph_t *graph = iterator->graph;
    result->distance = DISTANCE_SEND_NOW + spec->latency;

    /* Check if time to die */
    if (now == iterator->death_offset) {
        SET_DEAD(iterator);

        /* kill all messages waiting on his queue */
        result->msg      = MSG_DEATH;
        result->timeout  = 0;
        result->bitfield = BITFIELD_IGNORE_DATA;
        for (idx = 0; idx < iterator->in_queue.allocated; idx++) {
            send_item_t *stale = &iterator->in_queue.items[idx];
            if (stale->distance != DISTANCE_VACANT) {
                result->dst      = stale->src;
                result->src      = stale->dst;
                result->distance = stale->timeout - spec->step_index;
                ret              = global_enqueue(result, global_queue, spec->bitfield_size);
                if (ret != OK) {
                    return ret;
                }
            }
        }
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

int topology_iterator_omit(topology_iterator_t *iterator,
                           topo_funcs_t *funcs,
                           tree_recovery_method_t method,
                           node_id source,
                           int source_is_dead)
{
    if (iterator->graph == current_topology) {
        iterator->graph = comm_graph_clone(current_topology);
    }

    return funcs->fix_f(iterator->graph, iterator->ctx, method, source, source_is_dead);
}

void topology_iterator_destroy(topology_iterator_t *iterator, topo_funcs_t *funcs)
{
    if (iterator->in_queue.allocated) {
        free(iterator->in_queue.items);
        iterator->in_queue.items = NULL;
        iterator->in_queue.data = NULL;
        iterator->in_queue.allocated = 0;
    }

    funcs->stop_f(iterator->ctx);

    if (iterator->graph && (iterator->graph != current_topology)) {
        comm_graph_destroy(iterator->graph);
        iterator->graph = NULL;
    }

    assert(current_reference_count);
    if (current_topology && (--current_reference_count == 0)) {
        comm_graph_destroy(current_topology);
        current_topology = NULL;
    }
}
