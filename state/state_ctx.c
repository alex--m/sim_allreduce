#include <stdio.h>
#include "state.h"
#include "state_matrix.h"

typedef struct send_item
{
	node_id dst;
	node_id src;
	unsigned distance;
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

	unsigned node_count;        /* How many processes are simulated in total */
    topology_iterator_t *procs; /* Decides which way to send at every iteration */
	size_t per_proc_size;       /* The size of an iterator for a single process */

    unsigned char *new_matrix;  /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix;  /* Previous step of the matrix */

    send_list_t delayed;        /* Storing packets for future iterations (distance) */
    unsigned char *delayed_data;/* Bit-fields to be sent with a delay */
    optimization_t send;        /* Recycled buffers for sending */

	int verbose;
    unsigned death_timeout;     /* Steps it takes to "give up" and mark a node dead */
    tree_recovery_type_t recovery; /* Dead node recovery method */
	raw_stats_t stats;
} state_t;

#define GET_ITERATOR(ctx, proc) \
		((topology_iterator_t*)((char*)(ctx->procs) + (proc * ctx->per_proc_size)))

int state_create(topology_spec_t *spec, state_t *old_state, state_t **new_state)
{
	state_t *ctx;
	int index, ret_val;

    if (old_state) {
    	ctx = old_state;
    	memset(ctx->new_matrix, 0, CTX_MATRIX_SIZE(ctx));
    	memset(ctx->old_matrix, 0, CTX_MATRIX_SIZE(ctx));
    	memset(&ctx->stats, 0, sizeof(ctx->stats));

		for (index = 0; index < ctx->node_count; index++) {
			topology_iterator_destroy(GET_ITERATOR(ctx, index));
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
		ctx->node_count = spec->node_count;
		ctx->per_proc_size = topology_iterator_size();
		CTX_BITFIELD_SIZE(ctx) = CALC_BITFIELD_SIZE(ctx->node_count);

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

		ctx->procs = malloc(ctx->node_count * ctx->per_proc_size);
		if (ctx->procs == NULL)
		{
			state_destroy(ctx);
			return ERROR;
		}
    }

	for (index = 0; index < spec->node_count; index++) {
		topology_iterator_t *it = GET_ITERATOR(ctx, index);

		/* fill the initial bits (each node hold it's own data) */
		spec->my_rank = index;
    	spec->my_bitfield = GET_OLD_BITFIELD(ctx, index);
		SET_NEW_BIT(ctx, index, index);

		/* initialize the iterators over the topology requested */
		ret_val = topology_iterator_create(spec, it);
		if (ret_val != OK) {
			state_destroy(ctx);
			return ret_val;
		}

		if (!IS_DEAD(it)) {
			SET_LIVE(ctx, index);
		}
	}

	ctx->death_timeout = spec->death_timeout;
	ctx->recovery = spec->topology.tree.recovery;
	*new_state = ctx;
	return OK;
}

static inline int state_enqueue(state_t *state, node_id destination_rank,
		                        node_id source_rank, unsigned distance)
{
	unsigned slot_idx = 0, slot_size = CTX_BITFIELD_SIZE(state);
	send_list_t *list = &state->delayed;
	send_item_t *item;

	printf("ENQUEUE from %lu to %lu! (distance=%i)\n", source_rank, destination_rank, distance);

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
	item->src = source_rank;
	item->dst = destination_rank;
	item->distance = distance + 1; /* +1 because dequeue does (--distance == 0) */
	item->bitfield = state->delayed_data + (slot_idx * slot_size);
	memcpy(item->bitfield, GET_OLD_BITFIELD(state, source_rank),
			state->bitfield_size);
	list->used++;
	return OK;
}

static inline int state_send_message(state_t *state, node_id destination_rank,
		                             node_id source_rank, unsigned char *bitfield)
{
	int ret_val = OK;

	if (IS_LIVE(state, destination_rank)) {
		if (IS_LIVE_HERE(bitfield)) {
			/* live A sends to live B */
			MERGE(state, destination_rank, bitfield);
		} else {
			/* dead A sends to live B - simulates timeout on B */
			ret_val = topology_iterator_omit(GET_ITERATOR(state, destination_rank),
					state->recovery, source_rank);
		}
	} else {
		if (IS_LIVE_HERE(bitfield)) {
			/* live A sends to dead B - send back a notification (simulates timeout) */
			ret_val = state_enqueue(state, source_rank,
					destination_rank, state->death_timeout);
		} /* else: dead A sends to dead B - rare, if B died since his send */
	}

	return ret_val;
}

static inline int state_dequeue(state_t *state)
{
	send_item_t *item;
	unsigned slot_idx;
	send_list_t *delayed = &state->delayed;

	if (delayed->used == 0) {
		return OK;
	}

	for (slot_idx = 0; slot_idx < delayed->allocated; slot_idx++) {
		item = &delayed->items[slot_idx];
		if ((item->bitfield != NULL) && (--item->distance == 0)) {
			printf("DEQUEUE from %lu to %lu!\n", item->src, item->dst);
			state_send_message(state, item->dst, item->src, item->bitfield);
			item->bitfield = NULL; /* mark as no longer used */
			delayed->used--;
		}
	}

	return OK;
}

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_next_step(state_t *state)
{
	int ret_val;
	node_id idx;
	node_id dead_count = 0;
	node_id destination = (node_id)-1;
	node_id active_count = state->node_count;
	unsigned distance;

	/* switch step matrix before starting next iteration */
	memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

	/* Deliver queued packets */
	state_dequeue(state);

	/* iterate over all process-iterators */
	for (idx = 0; idx < state->node_count; idx++) {
		topology_iterator_t *iterator = GET_ITERATOR(state, idx);
		/* Get next the target rank of the next send */
		ret_val = topology_iterator_next(iterator, &destination, &distance);
		if (ret_val != OK) {
			if (ret_val == DONE) {
				active_count--;
			} else {
				return ret_val;
			}
		}

		if ((ret_val != DONE) && (distance != NO_PACKET)) {
			/* update the statistics */
			state->stats.messages_counter++;
			state->stats.data_len_counter += POPCOUNT(state, idx);

			/* determine which group contains this destination rank */
			if (distance == 0) {
				state_send_message(state, destination, idx,
						GET_OLD_BITFIELD(state, idx));
			} else {
				/* register it to some sending buffer */
				ret_val = state_enqueue(state, destination, idx, distance);
			}
		} else if ((distance == NO_PACKET) &&
				   (destination != (node_id)-1) &&
				   (IS_DEAD(GET_ITERATOR(state, destination)))) {
			/* take care of a delayed notification */
			ret_val = state_enqueue(state, idx, destination, state->death_timeout);
		}

		if (IS_DEAD(iterator)) {
			dead_count++;
		}

		if (ret_val != OK) {
			return ret_val;
		}

		/* optionally, output debug information */
		if (state->verbose == 1) {
			if (idx == 0) {
				printf("\n");
			}
			printf("\nproc=%3lu popcount:%3u\t", idx, POPCOUNT(state, idx));
			PRINT(state, idx);
			if (distance != NO_PACKET) {
				printf(" - sends to #%lu", destination);
			} else if (ret_val == DONE) {
				printf(" - Done!");
			} else if (destination == (node_id)-1) {
				printf(" - waits for somebody (bitfield incomplete)");
			} else {
				printf(" - waits for #%lu", destination);
			}
		}
	}

	printf("\nACTIVE=%lu DEAD=%lu\n", active_count, dead_count);
	return ((active_count - dead_count) > 0) ? OK : DONE;
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
    	for (i = 0; i < state->node_count; i++) {
    		topology_iterator_destroy(GET_ITERATOR(state, i));
    	}
    	free(state->procs);
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
