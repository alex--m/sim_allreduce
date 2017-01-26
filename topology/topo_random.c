#include "topology.h"

int random_build(topology_spec_t *spec, comm_graph_t **graph)
{
	*graph = NULL;
	return OK;
}

int random_start(topology_spec_t *spec, comm_graph_t *graph, topology_spec_t **internal_ctx)
{
	*internal_ctx = spec;
	return OK;
}

int random_fix(comm_graph_t *graph, void *internal_ctx, node_id broken)
{
	return OK;
}

int random_end(void *internal_ctx)
{
	return OK;
}

int random_next(comm_graph_t *graph, topology_spec_t *spec, node_id *target, unsigned *distance)
{
    unsigned next_target;
    unsigned cycle_len = 0;

    /* Select next target */
    switch (spec->topology_type)
    {
    case COLLECTIVE_TOPOLOGY_RANDOM_PURE:
        /* Send to a random node */
    	next_target = CYCLIC_RANDOM(spec, spec->node_count);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 2> random steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix - 1> const steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
        cycle_len = spec->topology.random.cycle_random + spec->topology.random.cycle_const;
        if ((spec->step_index % cycle_len) < spec->topology.random.cycle_random) {
        	next_target = CYCLIC_RANDOM(spec, spec->node_count);
        } else {
            /* Send to a node of increasing distance */
        	next_target = (spec->my_rank + spec->step_index) % spec->node_count;
        }
        break;

    /*
     * TODO: fix
	 */
//    case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC: /* Send to missing nodes from bit-field, the 1 radom for <radix> const steps */
//        if ((IS_MINE_FULL(ctx)) && (spec->topology.random.step_index % spec->topology.random.cycle_random))
//        {
//            next_target = CYCLIC_RANDOM(spec, spec->node_count);
//        } else if (IS_MINE_FULL(ctxs)) {
//            /* Send to a node of increasing distance */
//            next_target = (spec->my_rank + spec->topology.random.step_index) % spec->node_count;
//        } else {
//            /* Send to a random node missing from my (incoming) bitfield */
//            unsigned iter = CYCLIC_RANDOM(spec, spec->node_count - MY_POPCOUNT(ctx)) + 1;
//            next_target = 0;
//            while (iter) {
//                if (!IS_MY_BIT_SET(ctx, next_target)) {
//                    iter--;
//                }
//                next_target++;
//            }
//            next_target--;
//        }
//        break;

    default:
    	return ERROR;
    }

    /* Update step count, and cycle proportions for random hybrids */
    if (cycle_len && (spec->step_index % cycle_len == 0)) {
        switch (spec->topology_type)
        {
        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
            spec->topology.random.cycle_const++;
            break;

        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
            spec->topology.random.cycle_const <<= 1;
            break;

        default:
            break;
        }
    }

    *target = next_target;
    return OK;
}
