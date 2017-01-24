#include <stdio.h>
#include "state.h"
#include "state_matrix.h"

#ifdef MPI_SPLIT_PROCS
typedef struct exchange_plan
{
    unsigned packet_count;
    unsigned is_full;
} exchange_plan_t;
#endif

typedef struct send_list
{
	node_id *sources_and_destinations;
	unsigned allocated;
	unsigned used;
} send_list_t;

typedef struct state
{
	unsigned bitfield_size;     /* OPTIMIZATION */

	group_id my_group_index;    /* Index of the group expressed by this state*/
	unsigned local_node_count;  /* How many processes are expressed by this state */
    topology_iterator_t *procs; /* Decides which way to send at every iteration */

    unsigned char *new_matrix;  /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix;  /* Previous step of the matrix */

    unsigned group_count;       /* Total amount of groups */
    send_list_t *per_group;     /* Storing packets sent for the next iteration */
    send_list_t delayed;        /* Storing packets for future iterations (distance) */
    unsigned char *delayed_data;/* Bit-fields to be sent with a delay */
    optimization_t send;        /* Recycled buffers for sending */

	int verbose;
	raw_stats_t stats;
} state_t;

int state_create(topology_spec_t *spec, state_t *old_state, state_t **new_state)
{
	state_t *ctx;
	int index, ret_val;

    if (old_state) {
    	ctx = old_state;
    	memset(ctx->new_matrix, 0, CTX_MATRIX_SIZE(ctx));
    	memset(ctx->old_matrix, 0, CTX_MATRIX_SIZE(ctx));
    	memset(&ctx->stats, 0, sizeof(ctx->stats));
    	spec->my_bitfield = GET_OLD_BITFIELD(ctx, spec->my_rank);
    } else {
    	ctx = calloc(1, sizeof(*ctx));
		if (ctx == NULL) {
			return ERROR;
		}

		ctx->local_node_count = spec->local_node_count;
		CTX_BITFIELD_SIZE(ctx) = CALC_BITFIELD_SIZE(ctx->local_node_count);

		ctx->new_matrix = calloc(1, CTX_MATRIX_SIZE(ctx));
		if (ctx->new_matrix == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}

		ctx->old_matrix = calloc(1, CTX_MATRIX_SIZE(ctx));
		if (ctx->old_matrix == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}

		ctx->per_group = calloc(ctx->group_count, sizeof(send_list_t));
		if (ctx->per_group == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}

		ctx->procs = malloc(spec->node_count * sizeof(*(ctx->procs)));
		if (ctx->procs == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}
    }

	for (index = 0; index < spec->local_node_count; index++) {
		/* fill the initial bits (each node hold it's own data) */
		SET_BIT(ctx, index, index
		        + (ctx->local_node_count * ctx->my_group_index));

		/* initialize the iterators over the topology requested */
		spec->my_rank = index;
		ret_val = topology_iterator_create(spec, &ctx->procs[index]);
		if (ret_val != OK) {
			state_destroy(ctx);
			return ret_val;
		}
	}

	*new_state = ctx;
	return OK;
}

static inline int state_enqueue_ready(state_t *state, group_id destiantion_group,
		node_id destination_local_rank, node_id source_rank)
{
	send_list_t *list = &state->per_group[destiantion_group];

	/* make sure chuck has free slots */
	if (list->allocated == list->used) {
		/* extend chuck */
		list->allocated *= 2;
		list->sources_and_destinations = realloc(list->sources_and_destinations,
				2 * list->allocated * sizeof(*list->sources_and_destinations));
		if (!list->sources_and_destinations) {
			return ERROR;
		}
	}

	list->sources_and_destinations[2 * list->used] = source_rank;
	list->sources_and_destinations[2 * list->used + 1] = destination_local_rank;
	list->used++;
	return OK;
}

static inline int state_enqueue_delayed(state_t *state, group_id destiantion_group,
		node_id destination_local_rank, node_id source_rank, unsigned distance)
{
	send_list_t *list = &state->per_group[destiantion_group];

	/* make sure chuck has free slots */
	if (list->allocated == list->used) {
		/* extend chuck */
		list->allocated *= 2;
		list->sources_and_destinations = realloc(list->sources_and_destinations,
				2 * list->allocated * sizeof(*list->sources_and_destinations));
		if (!list->sources_and_destinations) {
			return ERROR;
		}

		state->delayed_data = realloc(state->delayed_data,
				list->allocated * state->bitfield_size);
		if (!state->delayed_data) {
			return ERROR;
		}
	}

	list->sources_and_destinations[2 * list->used] = source_rank;
	list->sources_and_destinations[2 * list->used + 1] = destination_local_rank;
	memcpy(state->delayed_data + (list->used * state->bitfield_size),
			GET_OLD_BITFIELD(state, source_rank), CTX_BITFIELD_SIZE(state));
	list->used++;
	return OK;
}

static inline int state_dequeue_delayed(state_t *state)
{
	// TODO: implement based on state_enqueue_delayed() ...
	return OK;
}

static inline int state_enqueue(state_t *state, group_id destiantion_group,
		node_id destination_local_rank, node_id source_rank, unsigned distance)
{
	return distance ? state_enqueue_delayed(state, destiantion_group, destination_local_rank, source_rank, distance) :
			state_enqueue_ready(state, destiantion_group, destination_local_rank, source_rank);
}

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(state_t *state, void **sendbuf,
                             int **sendcounts, int **sdispls,
                             unsigned long *total)
{
	int ret_val;
	unsigned distance;
	node_id destination;
	group_id destiantion_group;
	node_id destination_local_rank;
	unsigned idx, jdx, total_send_size;

	char *start, *send_iterator = state->send.buf;
	unsigned message_size = sizeof(node_id) + CTX_BITFIELD_SIZE(state);

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

	/* Iterate over all process-iterators */
	for (idx = 0; idx < state->local_node_count; idx++) {
		/* Get next the target rank of the next send */
		ret_val = topology_iterator_next(&state->procs[idx], &destination, &distance);
		if (ret_val != OK) {
			return ret_val;
		}

		if (distance != NO_PACKET) {
			/* Update the statistics */
			state->stats.messages_counter++;
			// TODO: state->stats.data_len_counter += MY_POPCOUNT(state);

			/* Determine which group contains this destination rank */
			destiantion_group = destination / state->local_node_count;
			destination_local_rank = destination % state->local_node_count;
			if (destiantion_group == state->my_group_index) {
				MERGE_LOCAL(state, destination_local_rank, destination_local_rank);
				return OK;
			}

			/* Register it to some sending buffer */
			ret_val = state_enqueue(state, destiantion_group,
					destination_local_rank, idx, distance);
			if (ret_val != OK) {
				return ret_val;
			}
		}

	    /* Optionally, output debug information */
	    if (state->verbose == 1) {
	        printf("\nproc=%3i popcount:%3i ", idx, POPCOUNT(state, idx));
	        PRINT(state, idx);
	    }
	}

	/* Check for delayed datagrams ready to be sent */
	ret_val = state_dequeue_delayed(state);
	if (ret_val != OK) {
		return ret_val;
	}

	/* Consolidate all the sending buffers into one for an MPI_Alltoallv() */
	for (total_send_size = 0, idx = 0; idx < state->group_count; idx++) {
		total_send_size += state->per_group[idx].used *
				(sizeof(node_id) + CTX_BITFIELD_SIZE(state));
	}

	if (total_send_size < state->send.buf_len) {
		state->send.buf_len *= 2;
		state->send.buf = realloc(state->send.buf, state->send.buf_len);
		if (!state->send.buf) {
			return ERROR;
		}
		send_iterator = state->send.buf;
	}

	for (idx = 0; idx < state->group_count; idx++) {
		node_id *node_iterator = state->per_group[idx].sources_and_destinations;
		start = send_iterator;

		/* create a message from each source+destination pair */
		for (jdx = 0; jdx < state->per_group[idx].allocated; jdx++) {
			*((node_id*)send_iterator) = *(node_iterator + 1);
			memcpy(((node_id*)send_iterator) + 1,
					GET_OLD_BITFIELD(state, *node_iterator),
					CTX_BITFIELD_SIZE(state));
			send_iterator += message_size;
			node_iterator += 2;
		}

		state->send.counts[idx] = send_iterator - start;
		state->send.displs[idx] = start - state->send.buf;
	}

	*sendbuf = state->send.buf;
	*sendcounts = state->send.counts;
	*sdispls = state->send.displs;
	*total = send_iterator - state->send.buf;
	return OK;
}

/* process a list of packets recieved from other peers (from MPI_Alltoallv) */
int state_process_next_step(state_t *state, const char *incoming, unsigned length)
{
	unsigned message_size = sizeof(node_id) + CTX_BITFIELD_SIZE(state);
	const char *limit = incoming + length;

	/* Iterate over the incoming buffers */
	for (; incoming < limit; incoming += message_size) {
        MERGE(state, *((node_id*)incoming), ((node_id*)incoming) + 1);
	}

    return IS_ALL_FULL(state);
}

int state_get_raw_stats(state_t *state, raw_stats_t *stats)
{
	memcpy(stats, &state->stats, sizeof(*stats));
	return OK;
}

/* Destroy state state_t*/
void state_destroy(state_t *state)
{
#ifdef MPI_SPLIT_PROCS
    free(ctx->targets);
    free(ctx->packets);
#endif

    if (state->per_group) {
    	unsigned i;
    	for (i = 0; i < state->local_node_count; i++) {
    		free(state->per_group[i].sources_and_destinations);
    	}
    	free(state->per_group);
    	state->per_group = NULL;
    }

    if (state->old_matrix) {
    	free(state->old_matrix);
    }
    if (state->new_matrix) {
    	free(state->new_matrix);
    }
    if (state->procs) {
    	free(state->procs);
    }
    free(state);
}
