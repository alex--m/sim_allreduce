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
    unsigned dest_local_rank;
    char bitfield[0];
} collective_datagram_t;

typedef struct send_chunck
{
	collective_datagram_t *sends;
	unsigned length_allocated;
	unsigned length_used;
} send_chunck_t;

typedef struct exchange_optimization
{
	void *buf;
	int buf_len;
	int dtype_len;
	int *counts;
	int *displs;
} optimization_t;

typedef struct state
{
	group_id my_group;          /* Index of the group expressed by this state*/
	unsigned local_proc_count;  /* How many processes are expressed by this state */
    topology_iterator_t *procs; /* Decides which way to send at every iteration */
    unsigned char *new_matrix;  /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix;  /* Previous step of the matrix */
    send_chunck_t *per_group;    /* Storing packets sent for the next iteration */
    send_chunck_t delayed;       /* Storing packets for future iterations (distance) */
    unsigned *time_offset;      /* Time from job start before starting collective */
    optimization_t send;        /* Recycled buffers for sending */
};

int state_create(collective_spec_t *spec, state_t *old_state, state_t **new_state)
{
	state_t *ctx;

	topology_iterator_t *initial = gen_topo(spec);

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
		if (ctx->storage_size) {
			collective_datagram_t *slot = ctx->storage;
			unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);
			for (index = 0; index < ctx->storage_size;
				 index++, slot = (collective_datagram_t*)((char*)slot + slot_size)) {
				slot->delay = 0;
			}
		} else {
			ctx->storage_size = 1;
			ctx->storage = malloc(sizeof(*ctx->storage) + CTX_BITFIELD_SIZE(ctx));
			if (ctx->storage == NULL) {
				PERROR("Allocation Failed!\n");
				return ERROR;
			}
			ctx->storage[0].delay = 0; // mark vacant
		}
	} else if (ctx->storage_size) {
		free(ctx->storage);
		ctx->storage_size = 0;
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

static inline int state_enqueue_ready(state_t state, node_id destination, group_id destiantion_group)
{
	collective_datagram_t *datagram;
	send_chunck_t chunck = state->per_group[destiantion_group];

	/* make sure chuck has free slots */
	if (chunck->length_allocated == chunck->length_used) {
		/* extend chuck */
		chunck->length_allocated *= 2;
		chunck->sends = realloc(chunck->sends, chunck->length_allocated);
		if (!chunck->sends) {
			return ERROR;
		}
	}

	/* store the new datagram */
	datagram = (char*)chunck->sends + chunck->length_used;
	datagram->dest_local_rank = destination;
	memcpy(datagram->bitfield, GET_OLD_BITFIELD(state, state->my_rank), CTX_BITFIELD_SIZE(state));
	chunck->length_used += CTX_BITFIELD_SIZE(state);

	return OK;
}

static inline int state_enqueue_delayed(state_t state, node_id destination, unsigned distance)
{
	send_chunck_t chuck = state->delayed;
	collective_datagram_t datagram;

	// TODO: implement a sorted linked-list of outgoing packets!

	return ERROR;
}

static inline int state_dequeue_delayed(state_t state)
{
	// TODO: implement based on state_enqueue_delayed() ...
	return OK;
}

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(state_t state, void **sendbuf, int **sendcounts, int **sdispls)
{
	int ret_val;
	unsigned distance;
	node_id destination;
	group_id destiantion_group;
	node_id destination_local_rank;
	unsigned idx, total_send_size;

	collective_datagram_t *iterator = state->send->buf;

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

	/* Iterate over all process-iterators */
	for (idx = 0; idx < state->local_proc_count; idx++) {
		/* Get next the target rank of the next send */
		ret_val = topology_iterator_next(state->procs[idx], &destination, &distance);
		if (ret_val != OK) {
			return ret_val;
		}

		/* Determine which group contains this destination rank */
		destiantion_group = destination / state->local_proc_count;
		destination_local_rank = destination % state->local_proc_count;
	    if (destiantion_group == state->my_group) {
			MERGE_LOCAL(state, destination_local_rank, destination_local_rank);
			return OK;
		}

		/* Register it to some sending buffer */
		ret_val = state_enqueue(state, destination, distance);
		if (ret_val != OK) {
			return ret_val;
		}

	    /* Optionally, output debug information */
	    if ((ctx->spec->test_count == 1) &&
	        (ctx->spec->proc_total_size < VERBOSE_PROC_THRESHOLD))
	    {
	        printf("\nstep=%2i proc=%3i popcount:%3i/%3i ", ctx->spec->step_index,
	               ctx->my_rank, POPCOUNT(ctx), ctx->spec->proc_total_size);
	        PRINT(ctx);
	    }
	}

	/* Check for delayed datagrams ready to be sent */
	ret_val = state_dequeue_delayed(state);
	if (ret_val != OK) {
		return ret_val;
	}

	/* Consolidate all the sending buffers into one for an MPI_Alltoallv() */
	for (total_send_size = 0, idx = 0; idx < state->local_proc_count; idx++) {
		struct send_chuck *parcel = state->per_dest->sends[idx];
		total_send_size += parcel->length_used;
	}

	if (total_send_size < state->send->buf_len) {
		state->send->buf_len *= 2;
		state->send->buf = realloc(state->send->buf, state->send->buf_len);
		if (!state->send->buf) {
			return ERROR;
		}
		iterator = state->send->buf;
	}

	for (idx = 0; idx < state->local_proc_count; idx++) {
		struct send_chuck *parcel = state->per_group->sends[idx];
		sdispls[idx + 1] = sdispls[idx] + parcel->length_used;
		memcpy(iterator, parcel->sends, parcel->length_used);
	}

	*sendbuf = state->send->buf;
	*sendcounts = state->send->counts;
	*sdispls = state->send->displs;
	return OK;
}

/* process a list of packets recieved from other peers (from MPI_Alltoallv) */
int state_process_next_step(state_t state, collective_datagram_t *incoming, unsigned count)
{
	collective_datagram_t *iterator = incoming;
	unsigned idx, datagram_size = CTX_DGRAM_SIZE(state);

	/* Iterate over the incoming buffers */
	for (idx = 0; idx < count; idx++) {
        MERGE(state, iterator->dest_local_rank, iterator->bitfield);
		(char*)iterator += datagram_size;
	}

    return IS_ALL_FULL(ctx);
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

    if (ctx->storage_size) {
        free(ctx->storage);
    }

    free(ctx->old_matrix);
    free(ctx->new_matrix);
    free(ctx);
}
