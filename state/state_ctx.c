#include "sim_allreduce.h"

#ifdef MPI_SPLIT_PROCS
typedef struct exchange_plan
{
    unsigned packet_count;
    unsigned is_full;
} exchange_plan_t;
#endif

typedef struct collective_datagram
{
    unsigned source_rank;
    unsigned dest_rank;
    unsigned delay;
    union {
        char bitfield[0];
        void *sent;
    };
} collective_datagram_t;

enum split_mode {
	NO_SPLIT = 0,
	SPLIT_ITERATIONS, /* every node does a portion of the iterations */
	SPLIT_PROCS, /* every node simulates a portion of procs in each iteration */
};

typedef struct rank_spec
{
    unsigned my_rank;          /* Global rank (0..N) */
    unsigned my_local_rank;    /* Local rank (< group_size) */
    enum split_mode mode;      /* The way the work is split among nodes */

    collective_spec_t *global_spec;   /* Properties of a single collective test */

} rank_spec_t;

typedef struct state
{
    unsigned char *new_matrix; /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix; /* Previous step of the matrix */

    unsigned *time_offset;     /* Time from job start before starting collective */
    unsigned planned;          /* Amount of packets stored */

    collective_datagram_t *storage; /* Storing packets sent on each iteration */
    unsigned stored;           /* Amount of packets stored */
};


int state_create(topology_iterator_t *initial, collective_spec_t *spec, state_t *old_state, state_t **new_state)
{
	state_t *ctx;

    sim_coll_stats_init(&spec->steps);
    sim_coll_stats_init(&spec->msgs);
    sim_coll_stats_init(&spec->data);

    if (!old_state) {
    	old_state = calloc(1, sizeof(*ctx));
		if (old_state == NULL) {
			return ERROR;
		}

		old_state->spec = spec;
		old_state->new_matrix = malloc(CTX_MATRIX_SIZE(ctx));
		if (old_state->new_matrix == NULL)
		{
			state_destroy(old_state, NULL);
			return ERROR;
		}

		old_state->old_matrix = malloc(CTX_MATRIX_SIZE(ctx));
		if (old_state->old_matrix == NULL)
		{
			state_destroy(old_state, NULL);
			return ERROR;
		}
    }

    ctx = old_state;

	memset(ctx->new_matrix, 0, CTX_MATRIX_SIZE(ctx));
	memset(ctx->old_matrix, 0, CTX_MATRIX_SIZE(ctx));

	if (ctx->spec->model == COLLECTIVE_MODEL_PACKET_DELAY) {
		if (ctx->stored) {
			collective_datagram_t *slot = ctx->storage;
			unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);
			for (index = 0; index < ctx->stored;
				 index++, slot = (collective_datagram_t*)((char*)slot + slot_size)) {
				slot->delay = 0;
			}
		} else {
			ctx->stored = 1;
			ctx->storage = malloc(sizeof(*ctx->storage) + CTX_BITFIELD_SIZE(ctx));
			if (ctx->storage == NULL) {
				PERROR("Allocation Failed!\n");
				return ERROR;
			}
			ctx->storage[0].delay = 0; // mark vacant
		}
	} else if (ctx->stored) {
		free(ctx->storage);
		ctx->stored = 0;
	}

	if (ctx->spec->model == COLLECTIVE_MODEL_TIME_OFFSET) {
		if (ctx->time_offset == NULL) {
			ctx->time_offset = malloc(ctx->spec->proc_group_size * sizeof(unsigned));
			if (ctx->time_offset == NULL) {
				PERROR("Allocation Failed!\n");
				return ERROR;
			}
		}

		for (index = 0; index < ctx->spec->proc_group_size; index++)
		{
			if (ctx->time_offset) {
				ctx->time_offset[index] = CYCLIC_RANDOM(ctx->spec,
														ctx->spec->offset_max);
			}
		}
	} else if (ctx->time_offset) {
		free(ctx->time_offset);
		ctx->time_offset = NULL;
	}

	// fill the initial bits
	SET_UNUSED_BITS(ctx);
	for (index = 0; index < ctx->spec->proc_group_size; index++)
	{
		SET_BIT(ctx, index, index +
				ctx->spec->proc_group_size * ctx->spec->proc_group_index);
	}

	*new_state = ctx;
	return OK;
}








static int sim_coll_send(unsigned dest_rank, unsigned distance,
                         collective_iteration_ctx_t* ctx, char *sent_bitfield)
{
#ifdef MPI_SPLIT_PROCS
    if (dest_rank / ctx->spec->proc_group_size != ctx->spec->proc_group_index) {
        collective_datagram_t *pkt = &ctx->packets[ctx->packet_count++];
        ctx->targets[dest_rank / ctx->spec->proc_group_size].packet_count++;
        pkt->source_rank = ctx->my_rank;
        pkt->dest_rank = dest_rank;
        pkt->sent = sent_bitfield;
        return OK;
    }
#endif

    if (distance) {
        void *new_storage;
        unsigned storage_iter = 0;
        collective_datagram_t *slot = ctx->storage;
        unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);

        /* Look for an available slot to store the message */
        while ((storage_iter < ctx->stored) && (slot->delay != 0)) {
            slot = (collective_datagram_t*)((char*)slot + slot_size);
            storage_iter++;
        }

        if (storage_iter == ctx->stored) {
            collective_datagram_t *extra_iter;

            /* Allocate extra storage space */
            ctx->stored <<= 1;
            new_storage = realloc(ctx->storage, ctx->stored * slot_size);
            if (!new_storage) {
                PERROR("Allocation Failed!\n");
                return ERROR;
            }

            /* Move to the new array of slots */
            slot = (collective_datagram_t*)(((char*)slot -
                    (char*)ctx->storage) + (char*)new_storage);
            ctx->storage = new_storage;
            extra_iter = slot;

            /* Mark the newest slots as free */
            while (storage_iter < ctx->stored) {
                extra_iter->delay = 0; /* Initialize the new slots as empty */
                extra_iter = (collective_datagram_t*)((char*)extra_iter + slot_size);
                storage_iter++;
            }
        }

        slot->source_rank = ctx->my_rank;
        slot->dest_rank = dest_rank;
        slot->delay = distance + 2;
        memcpy(&slot->bitfield, GET_OLD_BITFIELD(ctx, ctx->my_rank),
               CTX_BITFIELD_SIZE(ctx));
        return OK;
    }

    if (sent_bitfield) {
        MERGE(ctx, dest_rank, sent_bitfield);
    } else {
        MERGE_LOCAL(ctx, dest_rank, ctx->my_rank);
    }
    return OK;
}

static int sim_coll_process(collective_iteration_ctx_t *ctx)
{
    int res = OK;
    unsigned iter;
    unsigned next_target;
    unsigned expected_rank;
    unsigned distance = 0;
    unsigned cycle_len = 0;
    collective_plan_t *plan;

    if ((ctx->spec->model == COLLECTIVE_MODEL_PACKET_DROP) &&
        (ctx->spec->fail_rate > FLOAT_RANDOM(ctx->spec))) {
        ctx->spec->random_up++;
        return OK;
    }
    ctx->spec->random_down++;

    if ((ctx->spec->model == COLLECTIVE_MODEL_TIME_OFFSET) &&
        (ctx->spec->step_index < ctx->time_offset[ctx->my_local_rank])) {
        return OK;
    }

    if (ctx->spec->model == COLLECTIVE_MODEL_PACKET_DELAY) {
        distance = CYCLIC_RANDOM(ctx->spec, ctx->spec->delay_max);
    }


    switch (ctx->spec->topology)
    {
    case COLLECTIVE_TOPOLOGY_RANDOM_PURE:
        /* Send to a random node */
        next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 2> random steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix - 1> const steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
        cycle_len = ctx->spec->cycle_random + ctx->spec->cycle_const;
        if ((ctx->spec->step_index % cycle_len) < ctx->spec->cycle_random) {
            next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        } else {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + ctx->spec->step_index) %
                    ctx->spec->proc_total_size;
        }

        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC: /* Send to missing nodes from bit-field, the 1 radom for <radix> const steps */
        if ((IS_MINE_FULL(ctx)) && (ctx->spec->step_index % ctx->spec->tree_radix))
        {
            next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        } else if (IS_MINE_FULL(ctx)) {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + ctx->spec->step_index) %
                    ctx->spec->proc_total_size;
        } else {
            /* Send to a random node missing from my (incoming) bitfield */
            iter = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size - POPCOUNT(ctx)) + 1;
            next_target = 0;
            while (iter) {
                if (!IS_MY_BIT_SET(ctx, next_target)) {
                    iter--;
                }
                next_target++;
            }
            next_target--;
        }

        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    default:
        plan = ctx->plans[ctx->my_local_rank];

        /* Wait for incoming messages if planned */
        while ((plan->peer_next < plan->peer_count) &&
               (!plan->peer[plan->peer_next].is_send)) {
            expected_rank = plan->peer[plan->peer_next].peer_rank;
            if (IS_MY_BIT_SET(ctx, expected_rank)) {
                plan->peer_next++;
            } else {
                break;
            }
        }

        /* Send one outgoing message if planned */
        if ((plan->peer_next < plan->peer_count) &&
            (plan->peer[plan->peer_next].is_send)) {
            expected_rank = plan->peer[plan->peer_next].peer_rank;
            res = sim_coll_send(expected_rank, distance, ctx, NULL);
            plan->peer_next++;
        }
    }

    if (res != OK) {
        return res;
    }

    /* Update cycle proportions for random hybrids */
    if (cycle_len && (ctx->spec->step_index % cycle_len == 0)) {
        switch (ctx->spec->topology)
        {
        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
            ctx->spec->cycle_const++;
            break;

        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
            ctx->spec->cycle_const <<= 1;
            break;

        default:
            break;
        }
    }

    /* Treat pending messages if storage is relevant (distance between procs) */
    if (ctx->stored) {
        collective_datagram_t *slot;
        unsigned my_rank = ctx->my_rank;
        unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);

        /* Decrement send candidates */
        for (iter = 0, slot = ctx->storage;
             iter < ctx->stored;
             iter++,
             slot = (collective_datagram_t*)((char*)slot + slot_size)) {
            if ((slot->delay != 0) && (slot->source_rank == my_rank)) {
                slot->delay--;
                if (slot->delay == 1) {
                    res = sim_coll_send(slot->dest_rank, 0, ctx, &slot->bitfield[0]);
                    if (res != OK) {
                        return res;
                    }
                }
            }
        }
    }

    /* Optionally, output debug information */
    if ((ctx->spec->test_count == 1) &&
        (ctx->spec->proc_total_size < VERBOSE_PROC_THRESHOLD))
    {
        printf("\nstep=%2i proc=%3i popcount:%3i/%3i ", ctx->spec->step_index,
               ctx->my_rank, POPCOUNT(ctx), ctx->spec->proc_total_size);
        PRINT(ctx);
    }

    return res;
}


static int sim_coll_iteration(collective_iteration_ctx_t *ctx)
{
    int ret_val;

    /* Switch step matrix before starting next iteration */
    memcpy(ctx->old_matrix, ctx->new_matrix, CTX_MATRIX_SIZE(ctx));

    /* One iteration on each local node */
    ctx->my_local_rank = 0;
    ctx->my_rank = ctx->spec->proc_group_index * ctx->spec->proc_group_size;
    while (ctx->my_local_rank < ctx->spec->proc_group_size)
    {
        /* calculate next target */
        ret_val = sim_coll_process(ctx);
        if (ret_val == ERROR)
        {
            return ERROR;
        }

        ctx->my_local_rank++;
        ctx->my_rank++;
    }

#ifdef MPI_SPLIT_PROCS
    if (ctx->spec->proc_group_count > 1) {
        ret_val = sim_coll_mpi_exchange(ctx);
        if (ret_val == ERROR) {
            return ERROR;
        }
        return (ret_val == OK);
    }
#endif
    return IS_ALL_FULL(ctx);
}




/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(void **sendbuf, int **sendcounts, int **sdispls)
{

}

/* process a list of packets recieved from other peers (from MPI_Alltoallv) */
int state_process_next_step(void *recvbuf, int *recvcounts, int *rdispls)
{

}

/* Destroy state state_t*/
void state_destroy(state_t *ctx)
{
#ifdef MPI_SPLIT_PROCS
    free(ctx->targets);
    free(ctx->packets);
#endif

    if (ctx->plans) {
        unsigned i;
        for (i = 0; i < ctx->planned; i++) {
            free(ctx->plans[i]);
        }
        free(ctx->plans);
        ctx->plans = NULL;
    }

    if (ctx->time_offset) {
        free(ctx->time_offset);
    }

    if (ctx->stored) {
        free(ctx->storage);
    }

    free(ctx->old_matrix);
    free(ctx->new_matrix);
    free(ctx);
}
