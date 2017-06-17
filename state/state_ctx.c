#include <stdio.h>
#include <assert.h>
#include "state.h"
#include "state_matrix.h"

typedef struct state {
    unsigned bitfield_size;     /* OPTIMIZATION */

    topology_spec_t *spec;      /* Topology specification for this test round */
    topo_funcs_t *funcs;        /* List of functions used for iterating over nodes */
    topology_iterator_t *procs; /* Decides which way to send at every iteration */
    size_t per_proc_size;       /* The size of an iterator for a single process */

    unsigned char *new_matrix;  /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix;  /* Previous step of the matrix */

    send_list_t outq;           /* Storing packets for future iterations (distance) */
    raw_stats_t stats;          /* Result numbers and statistics of this round */
} state_t;

#define GET_ITERATOR(ctx, proc) \
        ((topology_iterator_t*)((char*)(ctx->procs) + (proc * ctx->per_proc_size)))

extern topo_funcs_t topo_map[];

int state_create(topology_spec_t *spec, state_t *old_state, state_t **new_state)
{
    state_t *ctx;
    int index, ret_val;

    if (old_state) {
        ctx = old_state;
        ctx->outq.used = 0;
        for (index = 0; index < ctx->outq.allocated; index++) {
        	ctx->outq.items[index].distance = DISTANCE_VACANT;
        }
        memset(ctx->new_matrix, 0, CTX_MATRIX_SIZE(ctx));
        memset(ctx->old_matrix, 0, CTX_MATRIX_SIZE(ctx));
        memset(&ctx->stats, 0, sizeof(ctx->stats));
        for (index = 0; index < spec->node_count; index++) {
            topology_iterator_destroy(GET_ITERATOR(ctx, index));
        }
    } else {
        ctx = calloc(1, sizeof(*ctx));
        if (ctx == NULL) {
            return ERROR;
        }

        ctx->spec = spec;
        ctx->per_proc_size = topology_iterator_size();
        spec->bitfield_size = CTX_BITFIELD_SIZE(ctx) =
        		CALC_BITFIELD_SIZE(spec->node_count);

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

        ctx->procs = malloc(spec->node_count * ctx->per_proc_size);
        if (ctx->procs == NULL)
        {
            state_destroy(ctx);
            return ERROR;
        }
    }

    /* Select and copy the function pointers for the requested topology */
    switch (spec->topology_type) {
    case COLLECTIVE_TOPOLOGY_NARRAY_TREE:
    case COLLECTIVE_TOPOLOGY_KNOMIAL_TREE:
    case COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE:
    case COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE:
        ctx->funcs = &topo_map[TREE];
        break;

    case COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING:
        ctx->funcs = &topo_map[BUTTERFLY];
        break;

    case COLLECTIVE_TOPOLOGY_ALL:
        return ERROR;
    }

    for (index = 0; index < spec->node_count; index++) {
        topology_iterator_t *it = GET_ITERATOR(ctx, index);

        /* fill the initial bits (each node hold it's own data) */
        spec->my_rank = index;
        spec->my_bitfield = GET_OLD_BITFIELD(ctx, index);
        SET_NEW_BIT(ctx, index, index);

        /* initialize the iterators over the topology requested */
        ret_val = topology_iterator_create(spec, ctx->funcs, it);
        if (ret_val != OK) {
            if (ret_val == DEAD) {
                printf("\nREAL-DEAD!\n");
                UNSET_LIVE(ctx, index);
            } else {
                state_destroy(ctx);
                return ret_val;
            }
        } else {
            SET_LIVE(ctx, index);
        }
    }

    *new_state = ctx;
    return OK;
}

static inline int state_enqueue(state_t *state, send_item_t *sent, send_list_t *list)
{
    unsigned slot_idx = 0, slot_size = CTX_BITFIELD_SIZE(state);
    send_item_t *item;

    if (sent->distance == DISTANCE_NO_PACKET) {
        return OK;
    }

    if (list == NULL) {
        list = &state->outq;
        if (IS_LIVE_HERE(sent->bitfield)) {
            state->stats.data_len_counter += POPCOUNT_HERE(sent->bitfield,
                    state->spec->node_count);
            state->stats.messages_counter++;
        }
    }

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

        list->data = realloc(list->data,
                list->allocated * slot_size);
        if (!list->data) {
            return ERROR;
        }

        /* Reset bitfield pointers to data */
		for (slot_idx = 0; slot_idx < list->used; slot_idx++) {
			list->items[slot_idx].bitfield =
					list->data + (slot_idx * slot_size);
		}

        /* set new slots as vacant */
        for (; slot_idx < list->allocated; slot_idx++) {
            list->items[slot_idx].distance = DISTANCE_VACANT;
            list->items[slot_idx].bitfield =
                    list->data + (slot_idx * slot_size);
        }

        /* Go to the first newly added slot */
        slot_idx = list->used;
    } else {
        /* find next slot available */
        while (list->items[slot_idx].distance != DISTANCE_VACANT) {
            slot_idx++;
        }
        assert(slot_idx < list->allocated);
    }

    /* fill the slot with the packet to be sent later */
    item = &list->items[slot_idx];
    memcpy(item, sent, offsetof(send_item_t, bitfield));
    memcpy(item->bitfield, sent->bitfield, slot_size);
    list->used++;
    return OK;
}

static inline int state_notify_dead(state_t *state, node_id dead, node_id target)
{
    send_item_t death = {
            .dst = target,
            .src = dead,
            .bitfield = GET_OLD_BITFIELD(state, dead) // Never actually merged!
    };

    death.distance = 2 * state->spec->topology.tree.radix; // TODO: calc_timeout();
    return state_enqueue(state, &death, NULL);
}

static inline int state_process(state_t *state, send_item_t *incoming, send_list_t *list)
{
    int ret_val = OK;
    topology_iterator_t *destination = GET_ITERATOR(state, incoming->dst);

    if (IS_DEAD(destination)) {
        /* Packet destination is dead */
        if (!IS_DEAD(GET_ITERATOR(state, incoming->src))) {
            /* live A sends to dead B - A needs to be "timed-out" (notified) */
            ret_val = state_notify_dead(state, incoming->dst, incoming->src);
        }
    } else {
        /* Packet destination is alive */
        if (IS_LIVE_HERE(incoming->bitfield)) {
            /* live A sends to live B */
			incoming->distance = DISTANCE_SEND_NOW;
			state_enqueue(state, incoming, list);
			incoming->distance = DISTANCE_VACANT;
        } else {
            /* dead A sends to live B - simulates timeout on B */
            ret_val = topology_iterator_omit(destination, state->funcs,
                    state->spec->topology.tree.recovery, incoming->src);
            SET_NEW_BIT(state, incoming->dst, incoming->src);
            // TODO: PROBLEM: if a dead X is marked "here", how do i wait for his children?!
        }
    }

    return ret_val;
}

static inline int state_dequeue(state_t *state)
{
    int ret_val = OK;
    send_item_t *item;
    send_list_t *outq = &state->outq;
    unsigned slot_idx, used = outq->used;

    for (slot_idx = 0, item = &outq->items[0];
         (used > 0) && (slot_idx < outq->allocated) && (ret_val == OK);
         slot_idx++, item++) {
        if (item->distance) {
            if (--item->distance == 0) {
                topology_iterator_t *iterator = GET_ITERATOR(state, item->dst);
                ret_val = state_process(state, item, &iterator->in_queue);
                outq->used--;
            }
            used--;
        }
    }

    return ret_val;
}

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_next_step(state_t *state)
{
    int ret_val;
    node_id idx;
    send_item_t res;
    node_id dead_count    = 0;
    topology_spec_t *spec = state->spec;
    topo_funcs_t *funcs   = state->funcs;
    node_id active_count  = spec->node_count;

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

    /* Deliver queued packets */
    state_dequeue(state);

    /* Iterate over all process-iterators */
    for (idx = 0; idx < state->spec->node_count; idx++) {
        topology_iterator_t *iterator = GET_ITERATOR(state, idx);
        if (IS_DEAD(iterator)) {
            res.distance = DISTANCE_NO_PACKET;
            res.dst = DESTINATION_DEAD;
            dead_count++;
            ret_val = OK; // For verbose mode
        } else {
            /* Get next the target rank of the next send */
            ret_val = topology_iterator_next(spec, funcs, iterator, &res);
            if (ret_val != OK) {
                if (ret_val == DONE) {
                    if (iterator->time_finished == 0) {
                        iterator->time_finished = state->spec->step_index;
                    }
                    active_count--;
                } else if (ret_val == DEAD) {
                    UNSET_LIVE(state, idx);
                } else {
                    return ret_val;
                }
            } if (res.distance != DISTANCE_NO_PACKET) {
				/* Send this outgoing packet */
				res.src = idx;
				res.bitfield = GET_OLD_BITFIELD(state, idx);
				ret_val = state_enqueue(state, &res, NULL);
				if (ret_val != OK) {
					return ret_val;
				}
            }
        }

        /* optionally, output debug information */
        if (state->spec->verbose) {
            if (idx == 0) {
                printf("\n");
            }
            printf("\nproc=%3lu popcount:%3u\t", idx, POPCOUNT(state, idx));
            PRINT(state, idx);
            if (ret_val == DONE) {
            	printf(" - Done!");
            } else if (res.distance != DISTANCE_NO_PACKET) {
            	if (res.dst == idx) {
            		printf(" - accepts from #%lu (type=%lu)", res.src, res.msg);
            	} else {
            		printf(" - sends to #%lu", res.dst);
            	}
            } else if (res.dst == DESTINATION_UNKNOWN) {
            	printf(" - waits for somebody (bitfield incomplete)");
            } else if (res.dst == DESTINATION_SPREAD) {
            	printf(" - pending (spread)");
            } else if (res.dst == DESTINATION_DEAD) {
                printf(" - DEAD");
            } else {
                printf(" - waits for #%lu", res.dst);
            }
        }
    }

    if ((active_count - dead_count) == 0) {
        state->stats.step_counter = state->spec->step_index;
        return DONE;
    }
    return OK;
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

    if (state->procs) {
        unsigned i;
        for (i = 0; i < state->spec->node_count; i++) {
            topology_iterator_destroy(GET_ITERATOR(state, i));
        }
        free(state->procs);
    }

    if (state->outq.allocated) {
        free(state->outq.items);
        free(state->outq.data);
        state->outq.allocated = 0;
        state->outq.used = 0;
    }
    if (state->old_matrix) {
        free(state->old_matrix);
    }
    if (state->new_matrix) {
        free(state->new_matrix);
    }
    free(state);
}
