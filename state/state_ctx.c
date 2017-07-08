#include <math.h>
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
    node_id dead_node;
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
            topology_iterator_destroy(GET_ITERATOR(ctx, index), ctx->funcs);
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

    /* Initialize individual (per-node) contexts */
    for (index = 0; index < spec->node_count; index++) {
        topology_iterator_t *it = GET_ITERATOR(ctx, index);

        /* fill the initial bits (each node hold it's own data) */
        spec->my_rank = index;
        spec->my_bitfield = GET_OLD_BITFIELD(ctx, index);
        SET_NEW_BIT(ctx, index, index);

        /* initialize the iterators over the topology requested */
        ret_val = topology_iterator_create(spec, ctx->funcs, it);
        if (ret_val != OK) {
        	state_destroy(ctx);
        	return ret_val;
        }
    }

    /* Choose dead (offline-fail) nodes, if applicable */
    if (((spec->model_type == COLLECTIVE_MODEL_NODES_MISSING) ||
    	 (spec->model_type == COLLECTIVE_MODEL_REAL)) &&
    	(spec->model.offline_fail_rate >= 1.0)){
		for (index = 0; index < spec->model.offline_fail_rate; index++) {
			do {
				dead_node = CYCLIC_RANDOM(spec, spec->node_count);
			} while (dead_node != 0);
			SET_DEAD(GET_ITERATOR(ctx, dead_node));
			printf("OFFLINE DEAD: %lu\n", dead_node);
		}
    }

    /* Choose faulty (online-fail) nodes, if applicable */
    if (((spec->model_type == COLLECTIVE_MODEL_NODES_FAILING) ||
    	 (spec->model_type == COLLECTIVE_MODEL_REAL)) &&
    	(spec->model.online_fail_rate >= 1.0)){
		for (index = 0; index < spec->model.online_fail_rate; index++) {
			do {
				dead_node = CYCLIC_RANDOM(spec, spec->node_count);
			} while (dead_node != 0);
			GET_ITERATOR(ctx, dead_node)->death_offset =
					CYCLIC_RANDOM(spec, spec->latency * (int)log10(spec->node_count));
			printf("ONLINE DEAD: %lu (at step #%lu)\n", dead_node, GET_ITERATOR(ctx, dead_node)->death_offset);
			// TODO: find a better upper-limit!
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
    	state->stats.data_len_counter += POPCOUNT_HERE(sent->bitfield,
    			state->spec->node_count);
    	state->stats.messages_counter++;
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
                list->allocated * sizeof(send_item_t));
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
    if (list->max < ++list->used) {
    	list->max = list->used;
    }
    return OK;
}

static inline int state_process(state_t *state, send_item_t *incoming)
{
    int ret_val = OK;
    topology_iterator_t *destination = GET_ITERATOR(state, incoming->dst);

    if (IS_DEAD(destination)) {
        /* Packet destination is dead */
    	if (incoming->timeout) {
    		/* Wait until the timeout to pronounce death */
    		send_item_t death;
    		memcpy(&death, incoming, sizeof(send_item_t));
    		death.distance = death.timeout - state->spec->step_index;
    		death.timeout = 0;
            ret_val = state_enqueue(state, &death, NULL);
        } else {
            /* dead A sends to live B - simulates timeout on B */
            ret_val = topology_iterator_omit(destination, state->funcs,
                    state->spec->topology.tree.recovery, incoming->src);
            SET_NEW_BIT(state, incoming->dst, incoming->src);
            if (POPCOUNT(state, incoming->dst) == state->spec->node_count) {
            	SET_FULL(state, incoming->dst);
            }
        }
    } else {
    	/* Packet destination is alive */
    	incoming->distance = DISTANCE_SEND_NOW;
    	ret_val = state_enqueue(state, incoming, &destination->in_queue);
    	incoming->distance = DISTANCE_VACANT;
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
        if (item->distance != DISTANCE_VACANT) {
            if (--item->distance == DISTANCE_VACANT) {
                ret_val = state_process(state, item);
                if (ret_val != OK) {
                	return ret_val;
                }
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
    node_id active_count  = spec->node_count - 1; // #0 is up forever

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

    /* Deliver queued packets */
    ret_val = state_dequeue(state);
    if (ret_val != OK) {
    	return ERROR; /* Don't forward the error in case it's "DONE" */
    }

    /* Iterate over all process-iterators */
    for (idx = 0; idx < state->spec->node_count; idx++) {
        topology_iterator_t *iterator = GET_ITERATOR(state, idx);
        if (IS_DEAD(iterator)) {
            res.distance = DISTANCE_NO_PACKET;
            res.dst = DESTINATION_DEAD;
            dead_count++;
            ret_val = OK; // For verbose mode
        } else if ((iterator->finish) && (idx != 0)) {
        	/* Already complete (for this node) */
        	ret_val = DONE;
            active_count--;
        } else {
            /* Get next the target rank of the next send */
        	res.src = idx; /* Also used for "protecting" #0 from death */
            ret_val = topology_iterator_next(spec, funcs, iterator, &res);
            if (ret_val != OK) {
                if (ret_val == DONE) {
                    if (iterator->finish == 0) {
                    	iterator->finish = state->spec->step_index;
                    } else {
                    	assert(idx == 0);
                    }
                    active_count--;
                } else {
                    return ret_val;
                }
            } else if (res.distance != DISTANCE_NO_PACKET) {
            	if (res.dst == idx) {
            		if (res.bitfield != BITFIELD_IGNORE_DATA) {
            			MERGE(state, idx, res.bitfield);
            		}
            	} else {
            		/* Send this outgoing packet */
            		res.src = idx;
            		res.bitfield = GET_OLD_BITFIELD(state, idx);
            		ret_val = state_enqueue(state, &res, NULL);
            		if (ret_val != OK) {
            			return ret_val;
            		}
            	}
            }
        }

        /* optionally, output debug information */
        if (state->spec->verbose) {
            if (idx == 0) {
                printf("\n");
            }
            if (IS_DEAD(iterator)) {
            	printf("\nproc=%lu\tpopcount=DEAD\t", idx);
            } else {
            	printf("\nproc=%lu\tpopcount=%u\t", idx, POPCOUNT(state, idx));
            }
            PRINT(state, idx);
            if (ret_val == DONE) {
            	printf(" - Done!");
            } else if (res.distance != DISTANCE_NO_PACKET) {
            	if (res.dst == idx) {
            		printf(" - accepts from #%lu (type=%lu)", res.src, res.msg);
            	} else {
            		printf(" - sends to #%lu (msg=%lu)", res.dst, res.msg);
            	}
            } else if (res.dst == DESTINATION_UNKNOWN) {
            	printf(" - waits for somebody (bitfield incomplete)");
            } else if (res.dst == DESTINATION_SPREAD) {
            	printf(" - pending (spread)");
            } else if (res.dst == DESTINATION_DEAD) {
                printf(" - DEAD");
            } else if (res.dst == DESTINATION_IDLE) {
                printf(" - IDLE!");
            } else {
                printf(" - waits for #%lu", res.dst);
            }
        }
    }

    if ((active_count - dead_count) == 0) {
    	topology_iterator_t *iterator   = GET_ITERATOR(state, 0);
        state->stats.max_queueu_len     = iterator->in_queue.max;
        state->stats.last_step_counter  = state->spec->step_index;
        state->stats.first_step_counter = iterator->finish;
        state->stats.death_toll         = dead_count;

        /* Find longest queue ever */
        for (idx = 1; idx < state->spec->node_count; idx++) {
        	iterator = GET_ITERATOR(state, idx);
        	if (state->stats.max_queueu_len < iterator->in_queue.max) {
        		state->stats.max_queueu_len = iterator->in_queue.max;
        	}
        }

        /* Find earliest finisher ever */
        // Note: Does NOT include #0 for spread calculation!
        for (idx = 1; idx < state->spec->node_count; idx++) {
        	iterator = GET_ITERATOR(state, idx);
        	if (state->stats.first_step_counter > iterator->finish) {
        		state->stats.first_step_counter = iterator->finish;
        	}
        }

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
void state_destroy(state_t *ctx)
{
    if (!ctx) {
        return;
    }

    if (ctx->procs) {
        unsigned i;
        for (i = 0; i < ctx->spec->node_count; i++) {
            topology_iterator_destroy(GET_ITERATOR(ctx, i), ctx->funcs);
        }
        free(ctx->procs);
    }

    if (ctx->outq.allocated) {
        free(ctx->outq.items);
        free(ctx->outq.data);
        ctx->outq.allocated = 0;
        ctx->outq.used = 0;
    }
    if (ctx->old_matrix) {
        free(ctx->old_matrix);
    }
    if (ctx->new_matrix) {
        free(ctx->new_matrix);
    }
    free(ctx);
}
