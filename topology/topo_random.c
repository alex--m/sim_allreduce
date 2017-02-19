#include "topology.h"
#include "../state/state_matrix.h"


struct random_ctx {
	topology_spec_t *spec;
	unsigned char *my_bitfield;
	node_id my_rank;
};

int random_build(topology_spec_t *spec, comm_graph_t **graph)
{
	*graph = comm_graph_create(0, 0);
	return *graph ? OK : ERROR;
}

int random_start(topology_spec_t *spec, comm_graph_t *graph, struct random_ctx **internal_ctx)
{
    *internal_ctx = malloc(sizeof(struct random_ctx));
    if (!*internal_ctx) {
        return ERROR;
    }

    (*internal_ctx)->spec = spec;
    (*internal_ctx)->my_rank = spec->my_rank;
    (*internal_ctx)->my_bitfield = spec->my_bitfield;
    return OK;
}

int random_fix(comm_graph_t *graph, struct random_ctx *internal_ctx, node_id broken)
{
	return OK;
}

int random_end(struct random_ctx *internal_ctx)
{
	free(internal_ctx);
	return OK;
}

int random_next(comm_graph_t *graph, struct random_ctx *ctx, node_id *target, unsigned *distance)
{
    unsigned next_target;
	topology_spec_t *spec = ctx->spec;
    unsigned cycle_len = spec->topology.random.cycle_random + spec->topology.random.cycle_const;

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
        if ((spec->step_index % cycle_len) < spec->topology.random.cycle_random) {
        	next_target = CYCLIC_RANDOM(spec, spec->node_count);
        } else {
            /* Send to a node of increasing distance */
        	next_target = (ctx->my_rank + spec->step_index) % spec->node_count;
        }
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC: /* Send to missing nodes from bit-field, the 1 radom for <radix> const steps */
        if ((IS_FULL_HERE(ctx->my_bitfield)) && (spec->step_index % cycle_len))
        {
            next_target = CYCLIC_RANDOM(spec, spec->node_count);
            printf("HERE1!\n");
        } else if (IS_FULL_HERE(ctx->my_bitfield)) {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + spec->step_index) % spec->node_count;
            printf("HERE2!\n");
        } else {
            /* Send to a random node missing from my (incoming) bitfield */
            unsigned iter = CYCLIC_RANDOM(spec, spec->node_count - POPCOUNT_HERE(ctx->my_bitfield, spec->node_count)) + 1;
            printf("HERE3!\n");
            next_target = 0;
            while (iter) {
                if (!IS_BIT_SET_HERE(next_target, ctx->my_bitfield)) {
                    iter--;
                }
                next_target++;
            }
            next_target--;
        }
        break;

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
