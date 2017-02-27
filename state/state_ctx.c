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

typedef struct send_item
{
	node_id dst;
	unsigned char *bitfield; // NULL means vacant
} send_item_t;

typedef struct send_list
{
	send_item_t *items;
	unsigned allocated;
	unsigned used;
} send_list_t;

typedef struct state
{
	unsigned bitfield_size;     /* OPTIMIZATION */

	group_id my_group_index;    /* Index of the group expressed by this state*/
	unsigned local_node_count;  /* How many processes are expressed by this state */
	unsigned global_node_count; /* How many processes are simulated in total */
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

		for (index = 0; index < ctx->local_node_count; index++) {
			topology_iterator_destroy(&ctx->procs[index]);
		}

		for (index = 0; index < ctx->group_count; index++) {
			ctx->per_group[index].used = 0;
		}

		for (index = 0; index < ctx->delayed.allocated; index++) {
			ctx->delayed.items[index].bitfield = NULL;
		}
    } else {
    	ctx = calloc(1, sizeof(*ctx));
		if (ctx == NULL) {
			return ERROR;
		}

		ctx->verbose = spec->verbose;
		ctx->my_group_index = spec->my_rank;
		ctx->global_node_count = spec->node_count;
		ctx->local_node_count = spec->local_node_count;
		ctx->group_count = spec->node_count / spec->local_node_count;
		CTX_BITFIELD_SIZE(ctx) = CALC_BITFIELD_SIZE(ctx->local_node_count);
    	memset(&ctx->stats, 0, sizeof(ctx->stats));

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

		ctx->procs = malloc(ctx->local_node_count * sizeof(*(ctx->procs)));
		if (ctx->procs == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}
    }

	for (index = 0; index < spec->local_node_count; index++) {
		/* fill the initial bits (each node hold it's own data) */
		spec->my_rank = index + (ctx->local_node_count * ctx->my_group_index);
    	spec->my_bitfield = GET_OLD_BITFIELD(ctx, spec->my_rank);
		SET_NEW_BIT(ctx, index, spec->my_rank);

		/* initialize the iterators over the topology requested */
		ret_val = topology_iterator_create(spec, &ctx->procs[index]);
		if (ret_val != OK) {
			state_destroy(ctx);
			return ret_val;
		}
	}

	*new_state = ctx;
	return OK;
}

static inline int state_enqueue_ready(state_t *state, group_id destination_group,
		node_id destination_local_rank, unsigned char* bitfield)
{
	send_list_t *list = &state->per_group[destination_group];
	send_item_t *item;

	/* make sure chuck has free slots */
	if (list->allocated == list->used) {
		/* extend chuck */
		if (list->allocated == 0) {
			list->allocated = 10;
		} else {
			list->allocated *= 2;
		}

		list->items = realloc(list->items, list->allocated * sizeof(*list->items));
		if (!list->items) {
			return ERROR;
		}
	}

	item = &list->items[list->used++];
	item->dst = destination_local_rank;
	item->bitfield = bitfield ;
	return OK;
}

static inline int state_enqueue_delayed(state_t *state, group_id destination_group,
		node_id destination_local_rank, node_id source_rank, unsigned distance)
{
	unsigned slot_idx = 0, slot_size = sizeof(group_id) + state->bitfield_size;
	send_list_t *list = &state->delayed;
	send_item_t *item;

	/* make sure chuck has free slots */
	if (list->allocated == list->used) {
		/* extend chuck */
		if (list->allocated == 0) {
			list->allocated = 10;
		} else {
			list->allocated *= 2;
		}

		list->items = realloc(list->items,
				list->allocated * sizeof(*list->items));
		if (!list->items) {
			return ERROR;
		}

		state->delayed_data = realloc(state->delayed_data,
				list->allocated * slot_size);
		if (!state->delayed_data) {
			return ERROR;
		}

		/* mark new slots as empty */
		for (slot_idx = list->used; slot_idx < list->allocated; slot_idx++) {
			list->items[slot_idx].bitfield = NULL;
		}

		slot_idx = list->used;
	}

	/* find next slot available */
	while (state->delayed.items[slot_idx].bitfield != NULL) {
		slot_idx++;
	}

	/* fill the slot with the packet to be sent later */
	item = &list->items[slot_idx];
	item->dst = destination_local_rank;
	item->bitfield = state->delayed_data + (slot_idx * slot_size) + sizeof(group_id);
	*(group_id*)(state->delayed_data + (slot_idx * slot_size)) = destination_group;
	memcpy(item->bitfield, GET_OLD_BITFIELD(state, source_rank), state->bitfield_size);
	list->used++;
	return OK;
}

static inline int state_dequeue_delayed(state_t *state)
{
	send_item_t *item;
	group_id destination_group;
	send_list_t *delayed = &state->delayed;
	unsigned slot_idx, slot_size = sizeof(group_id) + state->bitfield_size;

	for (slot_idx = 0; slot_idx < delayed->allocated; slot_idx++) {
		destination_group = *(group_id*)(state->delayed_data + (slot_idx * slot_size));
		if (-1 != destination_group) {
			item = &delayed->items[slot_idx];
			state_enqueue_ready(state, destination_group, item->dst, item->bitfield);
			item->bitfield = NULL; /* mark as no longer used */
		}
	}

	return OK;
}

static inline int state_enqueue(state_t *state, group_id destination_group,
		node_id destination_local_rank, node_id source_rank, unsigned distance)
{
	return distance ? state_enqueue_delayed(state, destination_group,
			destination_local_rank, source_rank, distance) :
			state_enqueue_ready(state, destination_group,
					destination_local_rank, GET_OLD_BITFIELD(state, source_rank));
}

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(state_t *state, void **sendbuf,
                             int **sendcounts, int **sdispls,
                             unsigned long *total)
{
	int ret_val;
	unsigned distance;
	node_id destination;
	group_id destination_group;
	node_id destination_local_rank;
	unsigned idx, jdx, total_send_size;
	char *start, *send_iterator = state->send.buf;

	/* a message is composed of a local node id and the sent bitfield */
	unsigned message_size = sizeof(node_id) + CTX_BITFIELD_SIZE(state);

    /* switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

	/* iterate over all process-iterators */
	for (idx = 0; idx < state->local_node_count; idx++) {
		destination = -2; /* set some default */

		/* Get next the target rank of the next send */
		ret_val = topology_iterator_next(&state->procs[idx], &destination, &distance);
		if (ret_val != OK) {
			return ret_val;
		}

		if (distance != NO_PACKET) {
			/* update the statistics */
			state->stats.messages_counter++;
			// TODO: state->stats.data_len_counter += MY_POPCOUNT(state);

			/* determine which group contains this destination rank */
			destination_group = destination / state->local_node_count;
			destination_local_rank = destination % state->local_node_count;
			if (destination_group == state->my_group_index) {
				MERGE_LOCAL(state, destination_local_rank, idx);
			} else {
				/* register it to some sending buffer */
				ret_val = state_enqueue(state, destination_group,
						destination_local_rank, idx, distance);
				if (ret_val != OK) {
					return ret_val;
				}
			}
		}

	    /* optionally, output debug information */
	    if (state->verbose == 1) {
	    	if (idx == 0) {
	    		printf("\n");
	    	}
	        printf("\nproc=%3i popcount:%3i\t", idx, POPCOUNT(state, idx));
	        PRINT(state, idx);
	        if (distance != NO_PACKET) {
		        printf(" - sends to #%lu", destination);
	        } else if (destination == (node_id)-1) {
		        printf(" - Done!");
	        } else if (destination == (node_id)-2) {
		        printf(" - Pass!");
	        } else {
		        printf(" - waits for #%lu", destination);
	        }
	    }
	}

	/* just this node - we're done! */
	if (state->group_count == 1) {
		*total = 0;
		return OK;
	}

	/* check for delayed datagrams ready to be sent */
	ret_val = state_dequeue_delayed(state);
	if (ret_val != OK) {
		return ret_val;
	}

	/* Consolidate all the sending buffers into one for an MPI_Alltoallv() */
	for (total_send_size = 0, idx = 0; idx < state->group_count; idx++) {
		total_send_size += state->per_group[idx].used *
				(sizeof(node_id) + CTX_BITFIELD_SIZE(state));
	}

	if ((total_send_size < state->send.buf_len) || (state->send.buf_len == 0)) {
		if (state->send.buf_len == 0) {
			state->send.buf_len = total_send_size;

			state->send.counts = calloc(state->group_count, sizeof(int));
			if (!state->send.counts) {
				return ERROR;
			}

			state->send.displs = calloc(state->group_count, sizeof(int));
			if (!state->send.displs) {
				return ERROR;
			}
		} else {
			state->send.buf_len *= 2;
			state->send.buf = realloc(state->send.buf, state->send.buf_len);
			if (!state->send.buf) {
				return ERROR;
			}
			send_iterator = state->send.buf;
		}
	}

	for (idx = 0; idx < state->group_count; idx++) {
		send_item_t *item = state->per_group[idx].items;
		start = send_iterator;

		/* create a message from each source+destination pair */
		for (jdx = 0; jdx < state->per_group[idx].allocated; jdx++) {
			*((node_id*)send_iterator) = item->dst;
			memcpy(((node_id*)send_iterator) + 1, item->bitfield,
					state->bitfield_size);
			send_iterator += message_size;
			item++;
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
	if (length) {
		const char *limit = incoming + length;
		unsigned message_size = sizeof(node_id) + CTX_BITFIELD_SIZE(state);

		/* Iterate over the incoming buffers */
		for (; incoming < limit; incoming += message_size) {
			MERGE(state, *((node_id*)incoming), ((node_id*)incoming) + 1);
		}
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
	if (!state) {
		return;
	}

	if (state->send.buf) {
		free(state->send.buf);
	}
	if (state->send.counts) {
		free(state->send.counts);
	}
	if (state->send.displs) {
		free(state->send.displs);
	}

	if (state->procs) {
    	unsigned i;
    	for (i = 0; i < state->local_node_count; i++) {
    		topology_iterator_destroy(&state->procs[i]);
    	}
    	free(state->procs);
	}

    if (state->per_group) {
    	unsigned i;
    	for (i = 0; i < state->group_count; i++) {
    		if (state->per_group[i].items != NULL) {
    			free(state->per_group[i].items);
    		}
    	}
    	free(state->per_group);
    }

    if (state->delayed.items) {
    	free(state->delayed.items);
    }
    if (state->delayed_data) {
    	free(state->delayed_data);
    }
    if (state->old_matrix) {
    	free(state->old_matrix);
    }
    if (state->new_matrix) {
    	free(state->new_matrix);
    }
    free(state);
}
