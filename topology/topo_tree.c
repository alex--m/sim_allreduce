#include <math.h>
#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

#define TREE_PACKET_CHOSEN (2) /* Must be different from OK, DONE or ERROR(s) */

#define TREE_NEPOTISM_FACTOR (2.0)
/* A number 1<X<=2, setting the level of favoring the "immediate relatives" in the
 * tree - compared to the rest of the nodes. In the case of node failure - a
 * node may need to handle 1-st degree, 2-nd degree and even more distant
 * relatives in the graph. This factor determines the ratio of "attention" each
 * distance recieves: for a factor of X, each degree Y recieves 1/(X^Y) of it.
 */

typedef unsigned tree_distance_t;
#define ANY_DISTANCE (0)
#define MIN_DISTANCE (1)

typedef struct tree_contact {
    node_id node;             /* His node-identifier in the graph */
    tree_distance_t distance; /* His distance from me in the graph */
    step_num last_seen;       /* Last time he sent me anything */
    step_num last_sent;       /* Last time I sent him anything */
    step_num timeout;         /* How long until I consider him dead */
#define TIMEOUT_NEVER ((step_num)-1)
    step_num his_timeout;     /* How long until he considers me dead (resets to NEVER) */
} tree_contact_t;

typedef struct tree_context {
    unsigned char *my_bitfield;
    size_t bitfield_size;
    node_id node_count;

    step_num *step_index;
    node_id my_node;
    step_num latency;
    unsigned radix;
    enum tree_service_cycle_method service_method;
    unsigned random_seed;

    unsigned order_indicator;
    unsigned next_wait_index;
    unsigned next_send_index;
    int is_resend;

    unsigned contacts_used;
    tree_contact_t *contacts;
} tree_context_t;


enum tree_msg_type {
    TREE_MSG_DATA          = 0,
#define TREE_MSG_DATA(distance)   (TREE_MSG_DATA          + TREE_MSG_MAX * distance)
	TREE_MSG_REDATA        = 1,
#define TREE_MSG_REDATA(distance) (TREE_MSG_REDATA        + TREE_MSG_MAX * distance)
    TREE_MSG_RESULT        = 2,
#define TREE_MSG_RESULT(distance) (TREE_MSG_RESULT        + TREE_MSG_MAX * distance)
    TREE_MSG_KEEPALIVE     = 3,
#define TREE_MSG_KA(distance)     (TREE_MSG_KEEPALIVE     + TREE_MSG_MAX * distance)
    TREE_MSG_MAX           = 4,
};

enum tree_action {
	TREE_SEND_RESULT,
	TREE_SEND_DATA,
    TREE_RECV,
    TREE_WAIT
};

struct order {
    enum comm_graph_direction_type direction;
    enum tree_action action;
};

struct order tree_order[] = {
		/* First - wait for children */
        {COMM_GRAPH_CHILDREN,       TREE_RECV},
        {COMM_GRAPH_EXTRA_CHILDREN, TREE_RECV},
#define ORDER_SUBTREE_DONE (1)

		/* Then - send to fathers (up the tree) */
        {COMM_GRAPH_FATHERS,        TREE_SEND_DATA},
        {COMM_GRAPH_EXTRA_FATHERS,  TREE_SEND_DATA},

		/* Next - wait for results (for verbose mode) */
        {COMM_GRAPH_FATHERS,        TREE_RECV},
        {COMM_GRAPH_EXTRA_FATHERS,  TREE_RECV},

		/* "meta-step" - make sure everybody made it */
		{COMM_GRAPH_EXCLUDE,        TREE_WAIT},

		/* Finally - send to children (down the tree) */
		{COMM_GRAPH_CHILDREN,       TREE_SEND_RESULT},
        {COMM_GRAPH_EXTRA_CHILDREN, TREE_SEND_RESULT}
};


static unsigned tree_count_peers_up(comm_graph_t *graph, node_id origin, tree_distance_t distance)
{
	comm_graph_direction_ptr_t dir_ptr = graph->nodes[origin].directions[COMM_GRAPH_FATHERS];

	if (distance == 1) {
		return dir_ptr->node_count;
	}

	if (dir_ptr->node_count > 1) {
		return (origin != 0);
	}

	return dir_ptr->node_count ? tree_count_peers_up(graph, dir_ptr->nodes[0], distance - 1) : 0;
}

static unsigned tree_count_peers_down(comm_graph_t *graph, node_id origin, tree_distance_t distance)
{
	comm_graph_direction_ptr_t dir_ptr = graph->nodes[origin].directions[COMM_GRAPH_CHILDREN];
	unsigned idx, count;

	if (distance == 1) {
		return dir_ptr->node_count;
	}

	for (count = 0, idx = 0; idx < dir_ptr->node_count; idx++) {
		count += tree_count_peers_down(graph, dir_ptr->nodes[idx], distance - 1);
	}

	return count;
}

static inline unsigned tree_count_peers(comm_graph_t *graph, node_id origin, tree_distance_t distance)
{
	return tree_count_peers_up(graph, origin, distance) + tree_count_peers_down(graph, origin, distance);
}

#define TREE_SERVICE_CYCLE_LENGTH(graph, node) (2 * tree_count_peers(graph, node, 1) * TREE_NEPOTISM_FACTOR)
static inline step_num tree_calc_timeout(comm_graph_t *graph, tree_context_t *ctx, node_id dest, tree_distance_t distance)
{
    assert(distance != ANY_DISTANCE);
    assert(distance >= MIN_DISTANCE);

    step_num latency = ctx->latency;
    unsigned immediate_peers = tree_count_peers(graph, dest, 1);
    step_num service_window_size = 2 * immediate_peers * TREE_NEPOTISM_FACTOR;
    unsigned distant_peers = (distance == 1) ? immediate_peers : tree_count_peers(graph, dest, distance);
    if (!distant_peers) {
    	return 0;
    }

    double how_many_service_cycles = distant_peers * pow(TREE_NEPOTISM_FACTOR, distance) / service_window_size;
    unsigned complete_service_cycles = (unsigned) how_many_service_cycles;
    step_num total_service = service_window_size * (complete_service_cycles +
    		((double)complete_service_cycles != how_many_service_cycles));
    return total_service + (2 * latency);
}

size_t tree_ctx_size()
{
    return sizeof(tree_context_t);
}

int tree_start(topology_spec_t *spec, comm_graph_t *graph, tree_context_t *ctx)
{
    comm_graph_node_t *my_node = &graph->nodes[spec->my_rank];
    comm_graph_direction_ptr_t dir_ptr;
    step_num timeout;
    unsigned idx, tmp;

    memset(ctx, 0, sizeof(*ctx));
    ctx->my_node = spec->my_rank;
    ctx->latency = spec->latency;
    ctx->step_index = &spec->step_index;
    ctx->random_seed = spec->random_seed;
    ctx->my_bitfield = spec->my_bitfield;
    ctx->node_count = spec->node_count;
    ctx->bitfield_size = spec->bitfield_size;
    ctx->radix = spec->topology.tree.radix;

    ctx->contacts_used =
            my_node->directions[COMM_GRAPH_FATHERS]->node_count +
            my_node->directions[COMM_GRAPH_CHILDREN]->node_count;
    ctx->contacts = calloc(ctx->contacts_used, sizeof(tree_contact_t));
    if (!ctx->contacts) {
        return ERROR;
    }

    /* Add fathers to the contact list */
    dir_ptr = my_node->directions[COMM_GRAPH_FATHERS];
    for (idx = 0; idx < dir_ptr->node_count; idx++) {
    	node_id node = dir_ptr->nodes[idx];
        timeout = tree_calc_timeout(graph, ctx, node, 1);
        ctx->contacts[idx].node = node;
        ctx->contacts[idx].distance = MIN_DISTANCE;
        ctx->contacts[idx].his_timeout = timeout;
        ctx->contacts[idx].timeout = timeout;
        ctx->contacts[idx].last_seen = 0;
    }

    /* Add children to contact list */
    tmp = dir_ptr->node_count;
    dir_ptr = my_node->directions[COMM_GRAPH_CHILDREN];
    for (; idx < tmp + dir_ptr->node_count; idx++) {
    	node_id node = dir_ptr->nodes[idx - tmp];
        timeout = tree_calc_timeout(graph, ctx, node, 1);
        ctx->contacts[idx].node = node;
        ctx->contacts[idx].distance = MIN_DISTANCE;
        ctx->contacts[idx].his_timeout = timeout;
        ctx->contacts[idx].timeout = timeout;
        ctx->contacts[idx].last_seen = 0;
    }

    return OK;
}

void tree_stop(tree_context_t *ctx)
{
	free(ctx->contacts);
}

static void tree_validate(tree_context_t *ctx)
{
	unsigned idx;
	for (idx = 0; idx < ctx->contacts_used; idx++) {
		if ((ctx->contacts[idx].distance != DISTANCE_VACANT) && (ctx->contacts[idx].last_seen)) {
			assert((ctx->contacts[idx].timeout == TIMEOUT_NEVER) ||
					(ctx->contacts[idx].timeout > ctx->contacts[idx].last_seen));
			assert((ctx->contacts[idx].his_timeout == TIMEOUT_NEVER) ||
					(ctx->contacts[idx].his_timeout > *ctx->step_index + ctx->latency));
		}
	}
}

static tree_distance_t tree_pick_service_distance(comm_graph_t *graph, tree_context_t *ctx)
{
    step_num step_index, each_cycle, factor;
    tree_distance_t distance;
    float tester, rand;

    switch (ctx->service_method) {
    case TREE_SERVICE_CYCLE_RANDOM:
        /*
         * Simplest concept: choose from a random distribution corresponding
         * to the ratio of service between distances.
         */
        tester = 1.0;
        distance = 0;
        rand = FLOAT_RANDOM(ctx);
        while (rand < tester) {
            tester /= 2.0;
            distance++;
        }
        break;

    case TREE_SERVICE_CYCLE_CALC:
        /*
         * A deterministic way to allocate service to distances:
         * The service-distance-allocation array is expanded, so that the
         * iterator can reach even that receive a fraction of a slot.
         */
        step_index = *ctx->step_index;
        each_cycle = TREE_SERVICE_CYCLE_LENGTH(graph, ctx->my_node);
        factor = 1 + (step_index / each_cycle);
        distance = (step_index * factor) / (each_cycle * factor);
        break;
    }
    return distance;
}

static inline int tree_contact_lookup(tree_context_t *ctx, node_id id,
                                      tree_distance_t distance,
                                      tree_contact_t **contact)
{
    assert(id != ctx->my_node);
    tree_contact_t *tmp;

    /* Look for the contact in the existing list */
    unsigned idx;
    for (idx = 0; idx < ctx->contacts_used; idx++) {
    	tmp = &ctx->contacts[idx];
        if (tmp->node == id) {
        	if (tmp->distance == DISTANCE_VACANT) {
        		return DONE;
        	}
            *contact = tmp;
            if (distance) {
                assert(distance == tmp->distance);
            }
            return OK;
        }
    }

    /* We only got here if the id is not in the list! */
    assert(distance != DISTANCE_VACANT);

    /* Contact not found - allocate new! */
    idx = ctx->contacts_used++;
    ctx->contacts = realloc(ctx->contacts,
            ctx->contacts_used * sizeof(tree_contact_t));
    if (!ctx->contacts) {
        return ERROR;
    }

    /* Initialize contact */
    *contact                = &ctx->contacts[idx];
    (*contact)->his_timeout = TIMEOUT_NEVER;
    (*contact)->timeout     = TIMEOUT_NEVER;
    (*contact)->distance    = distance;
    (*contact)->node        = id;
    (*contact)->last_sent   = 0;
    return OK;
}

static inline int tree_next_by_topology(comm_graph_t *graph,
                                        tree_context_t *ctx,
                                        send_item_t *result)
{
    int ret, is_res = 0;
    node_id next_peer;
    unsigned order_idx;
    tree_contact_t *contact;
    step_num current_step_index, timeout;
    unsigned wait_index = ctx->next_wait_index;
    unsigned send_index = ctx->next_send_index;
    comm_graph_node_t *my_node = &graph->nodes[ctx->my_node];

    for (order_idx = ctx->order_indicator; // TODO: increment order indicator with progress!
         order_idx < (sizeof(tree_order) / sizeof(*tree_order));
         order_idx++) {
        enum comm_graph_direction_type dir_type = tree_order[order_idx].direction;
        comm_graph_direction_ptr_t dir_ptr = my_node->directions[dir_type];
        switch (tree_order[order_idx].action) {
        case TREE_RECV:
            if (wait_index < dir_ptr->node_count) {
                while (wait_index < dir_ptr->node_count) {
                    next_peer = dir_ptr->nodes[wait_index];
                    if (IS_BIT_SET_HERE(next_peer, ctx->my_bitfield)) {
                        wait_index++;
                        ctx->next_wait_index++;
                    } else {
                    	/* Store the progress */
                    	ctx->next_send_index = send_index;
                    	ctx->next_wait_index = wait_index;
                    	ctx->order_indicator = order_idx;

                        result->dst = next_peer; // For verbose mode
                        return OK;
                    }
                }
            }

            if (wait_index >= dir_ptr->node_count) {
                wait_index -= dir_ptr->node_count;
            }
            break;

        case TREE_SEND_RESULT:
        	is_res = 1;
        	/* no break */
        case TREE_SEND_DATA:
            if (send_index < dir_ptr->node_count) {
            	do {
                    ctx->next_send_index++;
					next_peer = dir_ptr->nodes[send_index++];
					ret = tree_contact_lookup(ctx, next_peer, DISTANCE_VACANT, &contact);
            	} while ((ret == DONE) && (send_index < dir_ptr->node_count));

            	/* Check if finished sending (if the rest are dead) */
            	if (ret == DONE) {
            		send_index -= dir_ptr->node_count;
            		break;
            	} else if (ret != OK) {
                    return ret;
                }

            	/* Store the progress */
            	ctx->next_send_index = send_index;
            	ctx->next_wait_index = wait_index;
            	ctx->order_indicator = order_idx;

                /* Send a keep-alive message */
                timeout              = tree_calc_timeout(graph, ctx, next_peer, contact->distance);
                current_step_index   = *ctx->step_index;
                contact->timeout     = current_step_index + timeout;
                contact->last_sent   = current_step_index;
                contact->his_timeout = TIMEOUT_NEVER;
                result->dst          = next_peer;
                result->timeout      = current_step_index + timeout;
                result->msg          = is_res ? TREE_MSG_RESULT(contact->distance) :
                		               (ctx->is_resend ? TREE_MSG_REDATA(contact->distance) :
                		                                 TREE_MSG_DATA(contact->distance));
                return TREE_PACKET_CHOSEN;
            } else {
                send_index -= dir_ptr->node_count;
            }
            break;

        case TREE_WAIT:
            /* Wait for bitmap to be full before distributing */
            if (!IS_FULL_HERE(ctx->my_bitfield)) {
            	/* Store the progress */
            	ctx->next_send_index = send_index;
            	ctx->next_wait_index = wait_index;
            	ctx->order_indicator = order_idx;
                result->dst          = DESTINATION_UNKNOWN; // For verbose mode
                return OK;
            }
            break;
        }
    }

    /* No more packets to send - we're done here! (unless it's the master) */
    result->dst = DESTINATION_IDLE;
    return (ctx->my_node != 0) ? DONE : OK;
}

static inline int tree_handle_incoming_packet(tree_context_t *ctx,
                                              send_item_t *incoming)
{
    enum tree_msg_type msg_type = incoming->msg % TREE_MSG_MAX;
    tree_distance_t msg_distance = incoming->msg / TREE_MSG_MAX;
    tree_contact_t *contact;
    int is_new = 0;

    assert(incoming->dst == ctx->my_node);
    int ret = tree_contact_lookup(ctx, incoming->src, msg_distance, &contact);
    if (ret != OK) {
    	if (ret == DONE) {
    		is_new = 1;
    	    ret = tree_contact_lookup(ctx, incoming->src, msg_distance, &contact);
    		if (ret != OK) {
    			return ret;
    		}
    	} else {
    		return ret;
    	}
	}

    contact->timeout     = TIMEOUT_NEVER;
    contact->last_seen   = *ctx->step_index;
    contact->his_timeout = incoming->timeout;
    switch (msg_type){
    case TREE_MSG_DATA:
    	if (!is_new) {
    		break;
    	} /* Otherwise initial result already sent - and need to resend */
    	/* no break */

    case TREE_MSG_REDATA:
        ctx->next_send_index = 0;
        ctx->next_wait_index = 0;
        ctx->order_indicator = 0;
    	ctx->is_resend       = 1;
    	break;

    case TREE_MSG_RESULT:
    	break;

    case TREE_MSG_KEEPALIVE:
        /* Prevent keep=alive messages passing data */
        return DONE;

    default:
    	return ERROR;
    }
    return OK;
}

static inline int queue_get_msg_by_distance(tree_context_t *ctx,
                                            send_list_t *in_queue,
                                            msg_type msg,
                                            tree_distance_t distance,
                                            send_item_t *result)
{
    int ret;
    send_item_t *it;
    unsigned pkt_idx, used;
    for (pkt_idx = 0, it = in_queue->items, used = in_queue->used;
         (pkt_idx < in_queue->allocated) && (used > 0);
         pkt_idx++, it++) {
        if (it->distance != DISTANCE_VACANT) {
            assert(it->dst == ctx->my_node);
            if (((distance != ANY_DISTANCE) && (it->msg == (msg + (TREE_MSG_MAX * distance)))) ||
                ((distance == ANY_DISTANCE) && ((it->msg % TREE_MSG_MAX) == msg))) {
                memcpy(result, it, sizeof(send_item_t));
                ret = tree_handle_incoming_packet(ctx, it);
                if (ret != OK) {
                    if (ret == DONE) {
                        result->bitfield = BITFIELD_IGNORE_DATA; // For keep-alive messages
                    } else {
                        assert(0);
                    }
                }

                in_queue->used--;
                it->distance = DISTANCE_VACANT;
                return TREE_PACKET_CHOSEN;
            }
            used--;
        }
    }
    return OK;
}

static inline int tree_pending_keepalives(comm_graph_t *graph, tree_context_t *ctx, step_num *eta,
                                          tree_distance_t distance, send_item_t *result)
{
    step_num timeout, current_step_index = *ctx->step_index;
    tree_contact_t *contact, *max_urgency = NULL;
    unsigned idx;

    /* First for incoming keep-alives awaiting acknowledgment */
    for (idx = 0; idx < ctx->contacts_used; idx++) {
        contact = &ctx->contacts[idx];
        if (((contact->distance == distance) || (distance == ANY_DISTANCE)) &&
        	(contact->his_timeout != TIMEOUT_NEVER) &&
            (contact->distance != DISTANCE_VACANT)) {
            if (!max_urgency) {
                max_urgency = contact;
            } else if (max_urgency->his_timeout > contact->his_timeout) {
                max_urgency = contact;
            }
        }
    }

    if (max_urgency) {
        contact = max_urgency;
        goto send_keepalive;
    }

    /* Second, check if the first, partial ETA has elapsed */
    if (((eta[DATA_ETA_SUBTREE] < current_step_index) && // TODO: once SUBTREE arrives (late) - update the FULL-TREE ETA accordingly
         (ctx->order_indicator <= ORDER_SUBTREE_DONE)) ||
        (eta[DATA_ETA_FULL_TREE] < current_step_index)) {
        // TODO: Assert BASIC model? shouldn't happen unless delay or failure
        for (idx = 0; idx < ctx->contacts_used; idx++) {
            contact = &ctx->contacts[idx];
            if (((contact->distance == distance) ||
                 (distance == ANY_DISTANCE)) &&
                (contact->his_timeout != TIMEOUT_NEVER) &&
                (contact->distance != DISTANCE_VACANT)) {
                goto send_keepalive;
            }
        }
    }

    return OK;

send_keepalive:
    /* Send a keep-alive message */
    timeout              = tree_calc_timeout(graph, ctx, contact->node, contact->distance);
    contact->timeout     = current_step_index + timeout;
    contact->last_sent   = current_step_index;
    contact->his_timeout = TIMEOUT_NEVER;
    result->msg          = TREE_MSG_KA(contact->distance);
    result->timeout      = current_step_index + timeout;
    result->dst          = contact->node;
    return TREE_PACKET_CHOSEN;
}

int tree_next(comm_graph_t *graph, send_list_t *in_queue,
              tree_context_t *ctx, send_item_t *result)
{
    tree_distance_t distance;
    int ret;

    /* Before starting - assert algorithm assumptions to be valid */
    tree_validate(ctx);

    /* Step #1: If considered still in "computation" - respond only to keep-alives */
    if (graph == NULL) {
    	ret = queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_KEEPALIVE, ANY_DISTANCE, result);
    	if (ret) {
    		if (ret != TREE_PACKET_CHOSEN) {
    			return ret;
    		}

    		/* Reply */
    		node_id dst = result->src;
    		result->src = result->dst;
    		result->dst = dst;
    	} else {
    		/* Mark as waiting */
    	    result->distance = DISTANCE_NO_PACKET;
    	    result->dst      = DESTINATION_SPREAD;
    	}
    	return OK;
    }

    /* Step #1: If data can be sent - send it! (w/o reading incoming messages) */
    ret = tree_next_by_topology(graph, ctx, result);
    if (ret == TREE_PACKET_CHOSEN) {
        return OK;
    }
    if (ret != OK) {
        return ret; /* Typically "DONE" */
    }

    /* Step #2: Determine which tree-distance gets service this time */
    distance = tree_pick_service_distance(graph, ctx);

service_distance:
    /* Step #3: Data (from service-distance) comes first, then keep-alives */
    if (queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_DATA, distance, result) ||
    	queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_REDATA, distance, result) ||
    	queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_RESULT, distance, result) ||
        queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_KEEPALIVE, distance, result)) {
        assert(result->src != ctx->my_node);
        return OK;
    }

    /* Step #4: If its time is up - send a keep-alive within service-distance */
    ret = tree_pending_keepalives(graph, ctx, graph->nodes[ctx->my_node].data_eta, distance, result);
    if (ret == TREE_PACKET_CHOSEN) {
        return OK;
    }
    if (ret != OK) {
        return ret; /* Typically "ERROR" */
    }

    /* Step #5: Service any other distance (arbitrary) */
    if (distance != ANY_DISTANCE) {
        distance = ANY_DISTANCE;
        goto service_distance;
    }

    /* Nothing to do - remain idle */
    result->distance = DISTANCE_NO_PACKET;
    assert(in_queue->used == 0);
    return OK;
}

int tree_fix(comm_graph_t *graph, tree_context_t *ctx,
             tree_recovery_method_t recovery, node_id dead)
{
    comm_graph_direction_ptr_t dir;
    node_id me = ctx->my_node;
    tree_contact_t *contact;
    step_num dead_distance;
    int is_father_dead;
    node_id idx, extra;

    /* Reset the count on received nodes - so new nodes may be considered */
    ctx->next_wait_index = 0;

    /* Exclude the dead node from further sends */
    int ret = comm_graph_append(graph, me, dead, COMM_GRAPH_EXCLUDE);
    if (ret != OK) {
        return ret;
    }

    /* Detect whether the dead is above in the tree */
    is_father_dead = 0;
    dir = graph->nodes[me].directions[COMM_GRAPH_FATHERS];
    for (idx = 0; idx < dir->node_count; idx++) {
        if (dir->nodes[idx] == dead) {
            is_father_dead = 1;
        }
    }
    dir = graph->nodes[me].directions[COMM_GRAPH_EXTRA_FATHERS];
    for (idx = 0; idx < dir->node_count; idx++) {
        if (dir->nodes[idx] == dead) {
            is_father_dead = 1;
        }
    }

    /* Find the dead contact */
    ret = tree_contact_lookup(ctx, dead, DISTANCE_VACANT, &contact);
    if (ret != OK) {
    	if (ret == DONE) {
    		/* If a KA was followed by DATA - a node could be marked twice as dead */
    		return OK;
    	}
        return ret;
    }
    dead_distance = contact->distance + 1;

    //printf("\nOMIT: %lu omits %lu (%i)", ctx->my_node, dead, is_father_dead); // TODO: remove!

    /* Restructure the tree to disregard the dead node */
    switch (recovery) {
    case COLLECTIVE_RECOVERY_CATCH_THE_BUS: // TODO: implement!
    case COLLECTIVE_RECOVERY_BROTHER_FIRST: //TODO: implement!
        /* calc brother - reverse BFS */
        /* add brother as father, myself as his child */
    case COLLECTIVE_RECOVERY_FATHER_FIRST:
        if ((is_father_dead) && (ctx->my_node != 0)) {
        	/* the dead node is above me in the tree - join the first father */
        	node_id new_father = graph->nodes[dead].directions[COMM_GRAPH_FATHERS]->nodes[0];

        	/* Check if the new father is also an existing father */
            dir = graph->nodes[me].directions[COMM_GRAPH_FATHERS];
        	for (idx = 0; ((idx < dir->node_count) && (dir->nodes[idx] != new_father)); idx++);
        	if (idx < dir->node_count) {
        		break;
        	}

        	/* Add the new father to the list */
        	ret = comm_graph_append(graph, me, new_father, COMM_GRAPH_EXTRA_FATHERS);
        	if (ret != OK) {
        		return ret;
        	}

        	/* Set the distance for the new father */
        	ret = tree_contact_lookup(ctx, new_father, dead_distance, &contact);
        	if (ret != OK) {
        		return ret;
        	}
        } else {
            /* the dead node is in my sub-tree - adopt his children */
            idx = graph->nodes[me].directions[COMM_GRAPH_EXTRA_CHILDREN]->node_count;
            ret = comm_graph_copy(graph, dead, me, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_CHILDREN);
            if (ret != OK) {
                return ret;
            }

            for (; idx < graph->nodes[me].directions[COMM_GRAPH_EXTRA_CHILDREN]->node_count; idx++) {
                extra = graph->nodes[me].directions[COMM_GRAPH_EXTRA_CHILDREN]->nodes[idx];
                ret = tree_contact_lookup(ctx, extra, dead_distance, &contact);
                if (ret != OK) {
                    return ret;
                }
            }

            /* Reset send counter - need to resent all the data! */
            ctx->next_send_index = 0;
            ctx->next_wait_index = 0;
            ctx->order_indicator = 0;
        }
        break;


    case COLLECTIVE_RECOVERY_ALL:
        break;
    }

    /* Remove this contact */
    ret = tree_contact_lookup(ctx, dead, DISTANCE_VACANT, &contact);
    if (ret != OK) {
        return ret;
    }
    contact->distance = DISTANCE_VACANT;
    return ret;
}

int tree_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * N-array tree (tree_radix=k):
     * For every level, each of the nodes of the previous level is added k
     * children.
     *
     *    Graphic example:
     *    https://ccsweb.lanl.gov/~pakin/software/conceptual/userguide/n_002dary-tree-functions.html#n_002dary-tree-functions
     *
     *
     * K-nomial tree (tree_radix=k):
     * For every level, each of the nodes so far (not just the previous
     * level) is added (k-1) children. Each node can have children on
     * multiple levels.
     *
     *    Graphic example:
     *    https://ccsweb.lanl.gov/~pakin/software/conceptual/userguide/k_002\
     *        dnomial-tree-functions.html#k_002dnomial-tree-functions
     */

    int ret;
    unsigned node_count = spec->node_count;
    unsigned tree_radix = spec->topology.tree.radix;
    int is_knomial = ((spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_TREE) ||
                      (spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE));
    int is_multiroot = ((spec->topology_type == COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE) ||
                        (spec->topology_type == COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE));

    node_id child_count;
    node_id next_child;
    node_id next_father;
    node_id first_child = 1;
    node_id first_father = 0;

    assert(tree_radix);

    /* Create an empty graph */
    *graph = comm_graph_create(node_count, 1);
    if (!*graph) {
        return ERROR;
    }

    /* Account for the multi-root variant of the tree */
    if (is_multiroot) {
        for (next_father = 0; next_father < tree_radix; next_father++) {
            for (next_child = 0; next_child < tree_radix; next_child++) {
                if (next_father != next_child) {
                    ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_FATHERS);
                    if (ret != OK) {
                        comm_graph_destroy(*graph);
                        return ret;
                    }
                }
            }
        }
        first_child = tree_radix;
    }

    /* Build the entire graph */
    next_child = first_child;
    while (next_child < node_count) {
        for (child_count = 0; child_count < tree_radix; child_count++) {
            for (next_father = first_father;
                 (next_father < first_child) && (next_child < node_count);
                 next_father++, next_child++) {
                ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_CHILDREN);
                if (ret != OK) {
                    comm_graph_destroy(*graph);
                    return ret;
                }
            }
        }

        first_child += (first_child - first_father) * tree_radix;
        if (!is_knomial) {
            first_father = next_father;
        }
    }

    /* Calculate ETA until partial data (sub-tree only) is available */
    first_child = node_count;
    while (first_child--) {
        comm_graph_direction_ptr_t dir;
        step_num child_eta, eta = 0;

        /* Find the child arriving last */
        dir = (*graph)->nodes[first_child].directions[COMM_GRAPH_CHILDREN];
        for (next_child = 0; next_child < dir->node_count; next_child++) {
            child_eta = (*graph)->nodes[dir->nodes[next_child]].data_eta[DATA_ETA_SUBTREE];
            if (child_eta > eta) {
                eta = child_eta;
            }
        }

        /* Set the ETA for all the children */
        (*graph)->nodes[first_child].data_eta[DATA_ETA_SUBTREE] =
                dir->node_count ? eta + spec->latency + 2 : 0;
    }


    if (is_multiroot) {
        /* Find the "multi-root" child (for a root - other roots) arriving last */
        step_num child_eta, eta = (*graph)->nodes[0].data_eta[DATA_ETA_SUBTREE];
        comm_graph_direction_ptr_t dir = (*graph)->nodes[0].directions[COMM_GRAPH_FATHERS];
        for (next_child = 0; next_child < dir->node_count; next_child++) {
        	child_eta = (*graph)->nodes[dir->nodes[next_child]].data_eta[DATA_ETA_SUBTREE];
        	if (child_eta > eta) {
        		eta = child_eta;
        	}
        }

        /* Set the ETA for all the roots */
        eta += spec->latency + 1 + dir->node_count;
        for (first_child = 0; first_child < tree_radix; first_child++) {
            (*graph)->nodes[first_child].data_eta[DATA_ETA_FULL_TREE] = eta;
        }
    } else {
        (*graph)->nodes[0].data_eta[DATA_ETA_FULL_TREE] =
        (*graph)->nodes[0].data_eta[DATA_ETA_SUBTREE];
        first_child = 1;
    }

    /* Calculate ETA until full output (entire tree) is available */
    for (first_child = 0; first_child < node_count; first_child++) {
        step_num eta = (*graph)->nodes[first_child].data_eta[DATA_ETA_FULL_TREE] + spec->latency + 2;
        comm_graph_direction_ptr_t dir = (*graph)->nodes[first_child].directions[COMM_GRAPH_CHILDREN];
        for (next_child = 0; next_child < dir->node_count; next_child++) {
            (*graph)->nodes[dir->nodes[next_child]].data_eta[DATA_ETA_FULL_TREE] = (eta++);
        }
    }

    if (spec->verbose) {
        for (first_child = 0; first_child < node_count; first_child++) {
            printf("#%lu\tSubtree-ETA=%lu\tFull-tree-ETA=%lu\n", first_child,
                    (*graph)->nodes[first_child].data_eta[DATA_ETA_SUBTREE],
                    (*graph)->nodes[first_child].data_eta[DATA_ETA_FULL_TREE]);
        }
    }

    return OK;
}
