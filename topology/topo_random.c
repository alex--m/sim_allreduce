#include "topology.h"

struct random_ctx {
	topology_type_t type;
	unsigned my_rank;
	unsigned step_count;
	unsigned proc_count;
	unsigned cycle_random;
	unsigned cycle_const;
	unsigned random_seed;
};

int random_build(topology_spec_t spec, comm_graph_t **graph)
{
	*graph = NULL;
	return OK;
}

int random_start(topology_spec_t spec, comm_graph_t *graph, struct random_ctx **internal_ctx)
{
	struct random_ctx ctx = malloc(sizeof(struct random_ctx));
	if (!ctx) {
		return ERROR;
	}

	ctx->type = spec->topology;
	ctx->my_rank = spec->my_rank;
	ctx->step_count = 0;
	ctx->proc_count = spec->node_count;
	ctx->cycle_random = spec->random.cycle_random;
	ctx->cycle_const = spec->random.cycle_const;
	ctx->random_seed = spec->random.random_seed;
	*internal_ctx = ctx;
	return OK;
}



int random_next(struct random_ctx *ctx, node_id *target, unsigned *distance)
{
    unsigned next_target;
    unsigned cycle_len = 0;

    /* Select next target */
    switch (ctx->type)
    {
    case COLLECTIVE_TOPOLOGY_RANDOM_PURE:
        /* Send to a random node */
    	next_target = CYCLIC_RANDOM(ctx, ctx->proc_count);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 2> random steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix - 1> const steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
        cycle_len = ctx->cycle_random + ctx->cycle_const;
        if ((ctx->step_count % cycle_len) < ctx->cycle_random) {
        	next_target = CYCLIC_RANDOM(ctx, ctx->proc_count);
        } else {
            /* Send to a node of increasing distance */
        	next_target = (ctx->my_rank + ctx->step_count) % ctx->proc_count;
        }
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC: /* Send to missing nodes from bit-field, the 1 radom for <radix> const steps */
        if ((IS_MINE_FULL(ctx)) && (ctx->step_count % ctx->cycle_random))
        {
            next_target = CYCLIC_RANDOM(ctx, ctx->proc_count);
        } else if (IS_MINE_FULL(ctx)) {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + ctx->step_count) % ctx->proc_count;
        } else {
            /* Send to a random node missing from my (incoming) bitfield */
            unsigned iter = CYCLIC_RANDOM(ctx, ctx->proc_count - POPCOUNT(ctx)) + 1;
            next_target = 0;
            while (iter) {
                if (!IS_MY_BIT_SET(ctx, next_target)) {
                    iter--;
                }
                next_target++;
            }
            next_target--;
        }
        break;

    }

    /* Update step count, and cycle proportions for random hybrids */
    ctx->step_count++;
    if (cycle_len && (ctx->step_count % cycle_len == 0)) {
        switch (ctx->type)
        {
        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
            ctx->cycle_const++;
            break;

        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
            ctx->cycle_const <<= 1;
            break;

        default:
            break;
        }
    }

    *target = next_target;
    return OK;
}
