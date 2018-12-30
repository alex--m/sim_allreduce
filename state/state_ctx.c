
#include <math.h>
#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "state.h"
#include "state_matrix.h"

#define MEASUREMENT_ITERATIONS (10000)
#define WARMUP_ITERATIONS (100)

typedef struct state {
    unsigned bitfield_size;     /* OPTIMIZATION */

    topology_spec_t *spec;      /* Topology specification for this test round */
    topo_funcs_t *funcs;        /* List of functions used for iterating over nodes */
    topology_iterator_t *procs; /* Decides which way to send at every iteration */
    size_t per_proc_size;       /* The size of an iterator for a single process */
    long async_step_usec;       /* Step duration if "Async. mode" (microseconds), or 0 */
    struct timeval async_start; /* Context creation time, for "Async. mode" */

    unsigned char *new_matrix;  /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix;  /* Previous step of the matrix */

    send_list_t outq;           /* Storing packets for future iterations (distance) */
    raw_stats_t stats;          /* Result numbers and statistics of this round */
} state_t;

#define GET_ITERATOR(ctx, proc) \
	((topology_iterator_t*)((char*)((ctx)->procs) + ((proc) * (ctx)->per_proc_size)))

extern topo_funcs_t topo_map[];

static inline int state_async_send(state_t *state, send_item_t *sent)
{
	unsigned temp_size = sizeof(send_item_t) + CTX_BITFIELD_SIZE(state);
	send_item_t *temp = alloca(temp_size);
	memcpy(temp, sent, sizeof(send_item_t));
	if (sent->bitfield) {
		memcpy(temp+1, sent->bitfield, CTX_BITFIELD_SIZE(state));
	}
	temp->test_gen = state->spec->test_gen;
	return (MPI_SUCCESS == MPI_Send(temp, temp_size, MPI_BYTE, sent->dst, 0, MPI_COMM_WORLD)) ? OK : ERROR;
}

static inline int state_process(state_t *state, send_item_t *incoming);
static inline int state_async_recv(state_t *state)
{
	int ret, flag = 0;
	long unsigned test_gen = state->spec->test_gen;
	unsigned temp_size = sizeof(send_item_t) + CTX_BITFIELD_SIZE(state);
	send_item_t *temp = alloca(temp_size);
	do {
		ret = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
		if (ret != MPI_SUCCESS) {
			return ERROR;
		}
		if (!flag) {
			return OK;
		}

		ret = MPI_Recv(temp, temp_size, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		if (ret != MPI_SUCCESS) {
			return ERROR;
		}
	} while (temp->test_gen != test_gen);

	if (temp->bitfield) {
		temp->bitfield = (unsigned char*)(temp + 1);
	}
	return state_process(state, temp);
}

static inline long state_async_get_step_time(state_t *state)
{
	long step_usec;
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	if (rank < 2) {
		/* Create a temporary buffer to exchange */
		unsigned temp_size = sizeof(send_item_t) + CTX_BITFIELD_SIZE(state);
		send_item_t *temp = alloca(temp_size);
		memset(temp, 0, temp_size);
		temp->bitfield = (unsigned char*)(temp + 1);
		SET_BIT_HERE(temp->bitfield, 1 - rank);
		temp->dst = 1 - rank;
		temp->src = rank;
		temp->msg = 3;

		/* Run some warm-up iterations */
		int i;
		for (i = 0; i < WARMUP_ITERATIONS; i++) {
			int ret1, ret2, ret3;
			if (rank == 0) {
				ret2 = state_async_send(state, temp);
				ret1 = MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		        ret3 = state_next_step(state);
			} else {
				ret1 = MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		        ret2 = state_next_step(state);
				ret3 = state_async_send(state, temp);
			}
			if ((ret1 != MPI_SUCCESS) || (ret2 != OK) || (ret3 != OK)) {
				return ERROR;
			}
		}

		/* Measure the average step time in microseconds */
		struct timeval start, end;
		gettimeofday(&start, NULL);
		for (i = 0; i < MEASUREMENT_ITERATIONS; i++) {
			if (rank == 0) {
				state_async_send(state, temp);
				MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				state_next_step(state);
			} else {
				MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				state_next_step(state);
				state_async_send(state, temp);
			}
		}
		gettimeofday(&end, NULL);
		step_usec = (end.tv_usec - start.tv_usec +
				(end.tv_sec - start.tv_sec) * 1000000) / MEASUREMENT_ITERATIONS;
		if (state->spec->verbose) {
			printf("Rank #%i measured step to be %lu microseconds.\n", rank, step_usec);
		}
	}

	/* Broadcast the result of the measurement */
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Bcast(&step_usec, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	return step_usec;
}

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
        memset(ctx->new_matrix, 0, 2*CTX_MATRIX_SIZE(ctx));
        memset(&ctx->stats, 0, sizeof(ctx->stats));
        for (index = 0; index < spec->node_count; index++) {
            topology_iterator_destroy(GET_ITERATOR(ctx, index), ctx->funcs);
            if (ctx->spec->async_mode) {
            	break;
            }
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
        		((spec->async_mode ? 1 : ctx->spec->node_count)
        				* ctx->per_proc_size));
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

    case COLLECTIVE_TOPOLOGY_OPTIMAL_TREE:
        ctx->funcs = &topo_map[OPTIMAL];
        break;

    case COLLECTIVE_TOPOLOGY_DE_BROIJN:
        ctx->funcs = &topo_map[DE_BRUIJN];
        break;

    case COLLECTIVE_TOPOLOGY_HYPERCUBE:
        ctx->funcs = &topo_map[HYPERCUBE];
        break;

    case COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING:
        ctx->funcs = &topo_map[BUTTERFLY];
        break;

    case COLLECTIVE_TOPOLOGY_ALL:
        return ERROR;
    }

    if (spec->async_mode) {
		topology_iterator_t *it = GET_ITERATOR(ctx, 0);
    	spec->my_bitfield = GET_OLD_BITFIELD(ctx, 0);
    	ret_val = topology_iterator_create(spec, ctx->funcs, it);
    	if (ret_val != OK) {
    		state_destroy(ctx);
    		return ret_val;
    	}

    	if (ctx->async_step_usec == 0) {
        	gettimeofday(&ctx->async_start, NULL);
    		ctx->async_step_usec = 1; /* For internal processing */
    		ctx->async_step_usec = state_async_get_step_time(ctx);

    		topology_iterator_destroy(it, ctx->funcs);
        	ret_val = topology_iterator_create(spec, ctx->funcs, it);
        	if (ret_val != OK) {
        		state_destroy(ctx);
        		return ret_val;
        	}
    	}

    	gettimeofday(&ctx->async_start, NULL);
    	SET_NEW_BIT(ctx, 0, spec->my_rank);
    } else {
    	/* Initialize individual (per-node) contexts */
    	for (index = 0; index < spec->node_count; index++) {
    		/* fill the initial bits (each node hold it's own data) */
    		topology_iterator_t *it = GET_ITERATOR(ctx, index);
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
    }

    /* Choose dead (offline-fail) nodes, if applicable */
    if (((spec->model_type == COLLECTIVE_MODEL_NODES_MISSING) ||
    	 (spec->model_type == COLLECTIVE_MODEL_REAL)) &&
    	 (spec->model.offline_fail_rate >= 1.0)){
		for (index = 0; index < spec->model.offline_fail_rate; index++) {
			do {
				dead_node = CYCLIC_RANDOM(spec, spec->node_count);
			} while (dead_node == 0);
			if (spec->async_mode) {
				MPI_Bcast(&dead_node, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
				if (dead_node == spec->my_rank) {
					SET_DEAD(GET_ITERATOR(ctx, 0));
				}
			} else {
				SET_DEAD(GET_ITERATOR(ctx, dead_node));
			}
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
			if (spec->async_mode) {
				MPI_Bcast(&dead_node, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
				if (dead_node == spec->my_rank) {
					GET_ITERATOR(ctx, 0)->death_offset =
							CYCLIC_RANDOM(spec, spec->latency * (1 + (int)log10(spec->node_count)));
				}
			} else {
				GET_ITERATOR(ctx, dead_node)->death_offset =
						CYCLIC_RANDOM(spec, spec->latency * (1 + (int)log10(spec->node_count)));
			}
			// TODO: find a better upper-limit!
			if (spec->verbose) {
				printf("ONLINE DEAD: %lu (at step #%lu)\n", dead_node, GET_ITERATOR(ctx, dead_node)->death_offset);
			}
		}
    }

    if (spec->async_mode) {
    	MPI_Barrier(MPI_COMM_WORLD); /* For timing synchronization among processes */
    }

    *new_state = ctx;
    return OK;
}

static inline int state_enqueue(state_t *state, send_item_t *sent, send_list_t *list)
{
    unsigned slot_idx = 0, slot_size;
    send_item_t *item;
    unsigned char *data;

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
    	if (sent->msg != MSG_DEATH) {
    		/* Send packets that require no queuing */
    		if (state->spec->async_mode) {
    			return state_async_send(state, sent);
    		}

    		/* Count this message and it's length */
    		state->stats.messages_counter++;
    		assert(sent->bitfield != BITFIELD_IGNORE_DATA);
    		state->stats.data_len_counter += POPCOUNT_HERE(sent->bitfield,
    				state->spec->node_count);
    	}
    	list = &state->outq;
    }

    /* make sure chuck has free slots */
    slot_size = CTX_BITFIELD_SIZE(state);
    if (list->allocated == list->used) {
        /* extend chuck */
        list->allocated += 100;
        list->items = realloc(list->items,
                list->allocated * (sizeof(send_item_t) + slot_size));
        if (!list->items) {
            perror("Failed to (re)allocate queue");
            printf("Failed to allocate %u items of size %u\n", list->allocated, slot_size);
            return ERROR;
        }

        /* Copy old entries to new location */
        list->data = (unsigned char*)(list->items + list->allocated);
        memmove(list->data, list->items + list->used, list->used * slot_size);

        /* Reset bitfield pointers to data */
		for (slot_idx = 0, item = list->items, data = list->data;
		     slot_idx < list->used; slot_idx++, item++, data += slot_size) {
			item->bitfield = data;
		}

        /* set new slots as vacant */
        for (; slot_idx < list->allocated; slot_idx++, item++, data += slot_size) {
            item->distance = DISTANCE_VACANT;
            item->bitfield = data;
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
    if (sent->bitfield == BITFIELD_IGNORE_DATA) {
    	memset(item->bitfield, 0, slot_size);
    	SET_BIT_HERE(item->bitfield, sent->src);
    } else {
    	memcpy(item->bitfield, sent->bitfield, slot_size);
    }
    if (list->max < ++list->used) {
    	list->max = list->used;
    }
    list->next = ++slot_idx;
    return OK;
}

int global_enqueue(send_item_t *sent, send_list_t *queue, unsigned bitfield_size) {
	state_t hack_state = {0};
	hack_state.bitfield_size = bitfield_size;
	return state_enqueue(&hack_state, sent, queue);
}

static inline int state_process(state_t *state, send_item_t *incoming)
{
    int ret_val = OK;
    topology_iterator_t *destination = GET_ITERATOR(state,
    		state->spec->async_mode ? 0 : incoming->dst);

    if (incoming->msg == MSG_DEATH) {
    	node_id local_node = state->spec->async_mode ? 0 : incoming->dst;
    	assert((state->spec->async_mode) || (IS_DEAD(GET_ITERATOR(state, incoming->src))));
    	assert(state->spec->model_type > COLLECTIVE_MODEL_SPREAD);
    	SET_NEW_BIT(state, local_node, incoming->src);
    	if (POPCOUNT(state, local_node) == state->spec->node_count) {
    		SET_FULL(state, local_node);
    	}

    	return topology_iterator_omit(destination, state->funcs,
    			state->spec->topology.tree.recovery, incoming->src, 1);
    }

    if (IS_DEAD(destination)) {
    	if (incoming->timeout != TIMEOUT_NEVER) {
    		/* Packet destination is dead - Wait until the timeout to pronounce death */
    		send_item_t death;
    		death.dst      = incoming->src;
    		death.src      = incoming->dst;
    		death.msg      = MSG_DEATH;
    		death.distance = incoming->timeout - state->spec->step_index;
    		death.timeout  = 0;
    		death.bitfield = BITFIELD_IGNORE_DATA;
    		ret_val        = state_enqueue(state, &death, NULL);
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

    for (slot_idx = 0, item = outq->items;
         (used > 0) && (slot_idx < outq->allocated) && (ret_val == OK);
         slot_idx++, item++) {
        if (item->distance != DISTANCE_VACANT) {
            if (--(item->distance) == DISTANCE_VACANT) {
                if (state->spec->async_mode) {
                	ret_val = state_async_send(state, item);
                } else {
                	ret_val = state_process(state, item);
                }
                if (ret_val != OK) {
                	return ret_val;
                }
                outq->used--;
            }
            used--;
        }
    }

    if (state->spec->async_mode) {
    	return state_async_recv(state);
    }

    return ret_val;
}

extern step_num topology_max_offset;

int state_next_step(state_t *state)
{
    int ret_val;
    node_id idx, cnt;
    send_item_t res;
    node_id dead_count    = 0;
    topology_spec_t *spec = state->spec;
    topo_funcs_t *funcs   = state->funcs;
    node_id active_count;

    /* Deliver queued packets */
    ret_val = state_dequeue(state);
    if (ret_val != OK) {
    	return ERROR; /* Don't forward the error in case it's "DONE" */
    }

    /* Switch step matrix before starting next iteration */
    memcpy(state->old_matrix, state->new_matrix, CTX_MATRIX_SIZE(state));

    /* Iterate over all process-iterators */
    if (state->spec->async_mode) {
    	struct timeval now;
    	gettimeofday(&now, NULL);
    	spec->step_index = ((now.tv_sec - state->async_start.tv_sec) * 1000000 +
    		(now.tv_usec - state->async_start.tv_usec)) / state->async_step_usec;

        idx          = state->spec->my_rank;
        cnt          = idx + 1;
        active_count = 1;
    } else {
        idx          = 0;
        cnt          = state->spec->node_count;
        active_count = spec->node_count - 1;
    }

    for (; idx < cnt; idx++) {
        node_id local_rank = (state->spec->async_mode) ? 0 : idx;
        topology_iterator_t *iterator = GET_ITERATOR(state, local_rank);

        if (IS_DEAD(iterator)) {
            res.distance = DISTANCE_NO_PACKET;
            res.dst = DESTINATION_DEAD;
            dead_count++;
        } else {
            /* Get next the target rank of the next send */
        	res.src = idx; /* Also used for "protecting" #0 from death */
            ret_val = topology_iterator_next(spec, funcs, iterator, &state->outq, &res);
            if (ret_val != OK) {
                if (ret_val == DONE) {
                    if (iterator->finish == 0) {
                    	iterator->finish = state->spec->step_index;
                    }
                    active_count--;
                    ret_val = OK;
                }
            } else if (res.distance != DISTANCE_NO_PACKET) {
        		if (res.msg == MSG_DEATH) {
            		if (res.bitfield != BITFIELD_IGNORE_DATA) {
            			MERGE(state, local_rank, res.bitfield);
            		}
        			ret_val = topology_iterator_omit(GET_ITERATOR(state, local_rank),
        					state->funcs, state->spec->topology.tree.recovery, res.src, 0);
        		} else if (res.dst == idx) {
            		if (res.bitfield != BITFIELD_IGNORE_DATA) {
            			MERGE(state, local_rank, res.bitfield);
            		}
            	} else {
            		/* Send this outgoing packet */
            		res.src = idx;
            		res.bitfield = GET_OLD_BITFIELD(state, local_rank);
            		ret_val = state_enqueue(state, &res, NULL);
            	}
            }
            if ((ret_val != OK) && (ret_val != DONE)) {
            	printf("PROBLEM is %i\n", ret_val);
            	return ret_val;
            }
        }

        /* optionally, output debug information */
        if ((state->spec->verbose) && (!state->spec->async_mode)) {
        	if (state->spec->verbose > 1) {
        		if (idx == 0) {
        			printf("\n");
        		}
        		if (IS_DEAD(iterator)) {
        			printf("\nproc=%lu\tpopcount=DEAD\t", idx);
        		} else {
        			printf("\nproc=%lu\tpopcount=%u\t", idx, POPCOUNT(state, local_rank));
        		}
        		PRINT(state, local_rank);
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
        				assert(res.dst < state->spec->node_count);
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
        			printf(" - waits for #%lu (timeout=%lu)", res.dst, res.timeout);
        		}
        	}

        	printf("step #%lu (spread=%lu): active_count=%lu dead_count=%lu diff=%lu\n",
        			state->spec->step_index, topology_max_offset,
					active_count, dead_count, (active_count - dead_count) );
        }
    }

    if ((active_count - dead_count) == 0) {
    	topology_iterator_t *iterator   = GET_ITERATOR(state, 0);
        state->stats.max_queueu_len     = iterator->in_queue.max;
        state->stats.first_step_counter = state->spec->step_index;
        state->stats.last_step_counter  = state->spec->step_index;
        state->stats.death_toll         = dead_count;

        if (!state->spec->async_mode) {
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
            if (ctx->spec->async_mode) {
            	break;
            }
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
