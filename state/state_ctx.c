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

        ctx->new_matrix = calloc(1, (2 * CTX_MATRIX_SIZE(ctx)) +
        		(spec->node_count * ctx->per_proc_size));
        if (ctx->new_matrix == NULL)
        {
            free(ctx);
            return ERROR;
        }

        ctx->old_matrix = ctx->new_matrix + CTX_MATRIX_SIZE(ctx);
        ctx->procs = (topology_iterator_t*)(ctx->old_matrix + CTX_MATRIX_SIZE(ctx));
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
			} while (dead_node == 0);
			SET_DEAD(GET_ITERATOR(ctx, dead_node));
			if (spec->verbose) {
				printf("OFFLINE DEAD: %lu\n", dead_node);
			}
		}
    }

    /* Choose faulty (online-fail) nodes, if applicable */
    if (((spec->model_type == COLLECTIVE_MODEL_NODES_FAILING) ||
    	 (spec->model_type == COLLECTIVE_MODEL_REAL)) &&
    	(spec->model.online_fail_rate >= 1.0)){
		for (index = 0; index < spec->model.online_fail_rate; index++) {
			do {
				dead_node = CYCLIC_RANDOM(spec, spec->node_count);
			} while (dead_node == 0);
			GET_ITERATOR(ctx, dead_node)->death_offset =
					CYCLIC_RANDOM(spec, spec->latency * (1 + (int)log10(spec->node_count)));
			// TODO: find a better upper-limit!
			if (spec->verbose) {
				printf("ONLINE DEAD: %lu (at step #%lu)\n", dead_node, GET_ITERATOR(ctx, dead_node)->death_offset);
			}
		}
    }

    *new_state = ctx;
    return OK;
}

static inline int state_enqueue(state_t *state, send_item_t *sent, send_list_t *list)
{
    unsigned slot_idx = 0, slot_size;
    send_item_t *item;

    if (sent == NULL) {
    	/* Enqueue all items from list into the global list */
    	for (item = list->items; slot_idx < list->allocated; item++, slot_idx++) {
    		if (item->distance != DISTANCE_VACANT) {
    			int ret = state_enqueue(state, item, NULL);
    			item->distance = DISTANCE_VACANT;
    			if (ret != OK) {
    				return ret;
    			}
    		}
    	}
		list->used = 0;
		list->next = 0;
    	return OK;
    }

    if (sent->distance == DISTANCE_NO_PACKET) {
        return OK;
    }

    if (list == NULL) {
    	list = &state->outq;
    	if (sent->msg != MSG_DEATH) {
    		/* Count this message and it's length */
    		state->stats.messages_counter++;
    		assert(sent->bitfield != BITFIELD_IGNORE_DATA);
    		state->stats.data_len_counter += POPCOUNT_HERE(sent->bitfield,
    				state->spec->node_count);
    	}
    }

    /* make sure chuck has free slots */

    slot_size = state ? CTX_BITFIELD_SIZE(state) : 0;
    if (list->allocated == list->used) {
    	assert(state != 0); /* Hack: no way to get slot_size unless state is given... */

        /* extend chuck */
        if (list->allocated == 0) {
            list->allocated = 2 * state->spec->node_count;
        } else {
            list->allocated *= 2;
        }

        list->items = realloc(list->items,
                list->allocated * (sizeof(send_item_t) + slot_size));
        if (!list->items) {
            return ERROR;
        }

        /* Reset bitfield pointers to data */
        list->data = (unsigned char*)(list->items + list->allocated);
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
    	slot_idx = list->next;

        /* find next slot available */
        while ((slot_idx < list->allocated) &&
        	   (list->items[slot_idx].distance != DISTANCE_VACANT)) {
            slot_idx++;
        }

        if (slot_idx == list->allocated) {
        	slot_idx = 0;
        	while (list->items[slot_idx].distance != DISTANCE_VACANT) {
				slot_idx++;
			}
        	assert(slot_idx < list->allocated);
        }
    }

    /* fill the slot with the packet to be sent later */
    item = &list->items[slot_idx];
    memcpy(item, sent, offsetof(send_item_t, bitfield));
    memcpy(item->bitfield, sent->bitfield, slot_size);
    if (list->max < ++list->used) {
    	list->max = list->used;
    }
    list->next = ++slot_idx;
    return OK;
}

int global_enqueue(send_item_t *sent, send_list_t *queue) {
	return state_enqueue(NULL, sent, queue);
}

static inline int state_process(state_t *state, send_item_t *incoming)
{
    int ret_val = OK;
    topology_iterator_t *destination = GET_ITERATOR(state, incoming->dst);

    if (incoming->msg == MSG_DEATH) {
    	assert(IS_DEAD(GET_ITERATOR(state, incoming->dst)));
    	assert(state->spec->model_type > COLLECTIVE_MODEL_SPREAD);
    	SET_NEW_BIT(state, incoming->src, incoming->dst);
    	if (POPCOUNT(state, incoming->src) == state->spec->node_count) {
    		SET_FULL(state, incoming->src);
    	}

    	return topology_iterator_omit(GET_ITERATOR(state, incoming->src),
    			state->funcs, state->spec->topology.tree.recovery,
				GET_ITERATOR(state, incoming->dst), 1);
    }

    if (IS_DEAD(destination)) {
        /* Packet destination is dead - Wait until the timeout to pronounce death */
    	send_item_t death;
    	assert(incoming->timeout);
    	memcpy(&death, incoming, sizeof(send_item_t));
    	death.distance = death.timeout - state->spec->step_index;
    	death.msg      = MSG_DEATH;
    	death.timeout  = 0;
    	ret_val        = state_enqueue(state, &death, NULL);
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

extern step_num topology_max_offset;

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

    /* Deliver queued packets */
    ret_val = state_dequeue(state);
    if (ret_val != OK) {
    	return ERROR; /* Don't forward the error in case it's "DONE" */
    }

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

    /* Iterate over all process-iterators */
    for (idx = 0; idx < state->spec->node_count; idx++) {
        topology_iterator_t *iterator = GET_ITERATOR(state, idx);
        if (IS_DEAD(iterator)) {
            res.distance = DISTANCE_NO_PACKET;
            res.dst = DESTINATION_DEAD;
            dead_count++;
        } else if ((iterator->finish) && (idx != 0)) {
        	/* Already complete (for this node) */
            active_count--;
        	ret_val = DONE;
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
                    ret_val = OK;
                }
            } else if (res.distance != DISTANCE_NO_PACKET) {
        		if (res.msg == MSG_DEATH) {
            		if (res.bitfield != BITFIELD_IGNORE_DATA) {
            			MERGE(state, idx, res.bitfield);
            		}
        			ret_val = topology_iterator_omit(GET_ITERATOR(state, idx),
        					state->funcs, state->spec->topology.tree.recovery,
							GET_ITERATOR(state, res.src), 0);
        		} else if (res.dst == idx) {
            		if (res.bitfield != BITFIELD_IGNORE_DATA) {
            			MERGE(state, idx, res.bitfield);
            		}
            	} else {
            		/* Send this outgoing packet */
            		res.src = idx;
            		res.bitfield = GET_OLD_BITFIELD(state, idx);
            		ret_val = state_enqueue(state, &res, NULL);
            	}
            }
            if ((ret_val != OK) && (ret_val != DONE)) {
            	printf("PROBLEM is %i\n", ret_val);
            	return ret_val;
            }
        }

        /* optionally, output debug information */
        if (state->spec->verbose > 1) {
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
            		if (res.msg == MSG_DEATH) {
            			printf(" - accepts from #%lu (includes DEATH NOTIFICATION!)", res.src);
            		} else {
            			printf(" - accepts from #%lu (type=%lu)", res.src, res.msg);
            		}
            	} else {
            		printf(" - sends to #%lu (msg=%lu)", res.dst, res.msg);
            	}
            } else if (res.dst == DESTINATION_UNKNOWN) {
            	printf(" - waits for somebody - max timeout is %lu (now is %lu)", res.src, spec->step_index);
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

    if (state->spec->verbose) {
    	printf("step #%lu (spread=%lu): active_count=%lu dead_count=%lu diff=%lu\n",
    			state->spec->step_index, topology_max_offset,
				active_count, dead_count, (active_count - dead_count) );
    }
    if ((active_count - dead_count) == 0) {
    	topology_iterator_t *iterator   = GET_ITERATOR(state, 0);
        state->stats.max_queueu_len     = iterator->in_queue.max;
        state->stats.first_step_counter = state->spec->step_index;
        state->stats.last_step_counter  = state->spec->step_index;
        state->stats.death_toll         = dead_count;

        /* Find longest queue ever */
        for (idx = 1; idx < state->spec->node_count; idx++) {
        	iterator = GET_ITERATOR(state, idx);
        	if ((!IS_DEAD(iterator)) &&
        	    (state->stats.max_queueu_len < iterator->in_queue.max)) {
        		state->stats.max_queueu_len = iterator->in_queue.max;
        	}
        }

        /* Find earliest finisher ever */
        // Note: Does NOT include #0 for spread calculation!
        for (idx = 1; idx < state->spec->node_count; idx++) {
        	iterator = GET_ITERATOR(state, idx);
        	if ((!IS_DEAD(iterator)) &&
        		(state->stats.first_step_counter > iterator->finish)) {
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
    }

    if (ctx->outq.allocated) {
        free(ctx->outq.items);
        ctx->outq.allocated = 0;
        ctx->outq.used = 0;
    }
    if (ctx->new_matrix) {
        free(ctx->new_matrix);
    }
    free(ctx);
}
