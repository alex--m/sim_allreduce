#include <math.h>
#include <assert.h>
#include "topology.h"
#include "../state/state_matrix.h"

#define TREE_PACKET_CHOSEN (2) /* Must be different from OK, DONE or ERROR(s) */

#define TREE_NEPOTISM_FACTOR (2)
/* A number >=2, setting the level of favoring the "immediate relatives" in the
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

    unsigned contacts_used;
    tree_contact_t *contacts;
} tree_context_t;


enum tree_msg_type {
    TREE_MSG_DATA          = 0,
#define TREE_MSG_DATA(distance) (TREE_MSG_DATA          + TREE_MSG_MAX * distance)
    TREE_MSG_KEEPALIVE     = 1,
#define TREE_MSG_KA(distance)   (TREE_MSG_KEEPALIVE     + TREE_MSG_MAX * distance)
	TREE_MSG_MAX           = 2,
};

enum tree_action {
    TREE_SEND,
    TREE_RECV,
    TREE_WAIT
};

struct order {
    enum comm_graph_direction_type direction;
    enum tree_action action;
};

struct order tree_order[] = {
        {COMM_GRAPH_CHILDREN,       TREE_RECV},
#define ORDER_SUBTREE_DONE (0)
        {COMM_GRAPH_FATHERS,        TREE_SEND},
        {COMM_GRAPH_EXTRA_FATHERS,  TREE_SEND},
        {COMM_GRAPH_MR_CHILDREN,    TREE_RECV},
        {COMM_GRAPH_EXTRA_FATHERS,  TREE_RECV},
        {COMM_GRAPH_FATHERS,        TREE_RECV},
        {COMM_GRAPH_EXCLUDE,        TREE_WAIT},
        {COMM_GRAPH_CHILDREN,       TREE_SEND},
        {COMM_GRAPH_EXTRA_CHILDREN, TREE_SEND}
};

#define TREE_SERVICE_CYCLE_LENGTH(ctx) (tree_calc_timeout(ctx, 1) * TREE_NEPOTISM_FACTOR)
static inline step_num tree_calc_timeout(tree_context_t *ctx, tree_distance_t distance)
{
	step_num latency = ctx->latency;
	unsigned radix = ctx->radix;
	assert(distance != ANY_DISTANCE);
	assert(distance >= MIN_DISTANCE);
	return (pow(TREE_NEPOTISM_FACTOR, distance-1) *
		    (pow(radix, distance) + 1) / (radix + 1)) + (2 * latency);
}

size_t tree_ctx_size() {
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
	timeout = tree_calc_timeout(ctx, 1);

	 // TODO: remove!
	for (idx = 10; idx > 0; idx--) {
		timeout = tree_calc_timeout(ctx, idx);
		printf("tree_calc_timeout[%i]: %lu\n", idx, timeout);
	}

	ctx->contacts_used =
			my_node->directions[COMM_GRAPH_FATHERS]->node_count +
			my_node->directions[COMM_GRAPH_CHILDREN]->node_count +
			my_node->directions[COMM_GRAPH_MR_CHILDREN]->node_count;
	ctx->contacts = calloc(ctx->contacts_used, sizeof(tree_contact_t));
	if (!ctx->contacts) {
		return ERROR;
	}

	/* Add fathers to the contact list */
	dir_ptr = my_node->directions[COMM_GRAPH_FATHERS];
	for (idx = 0; idx < dir_ptr->node_count; idx++) {
		ctx->contacts[idx].node = dir_ptr->nodes[idx];
		ctx->contacts[idx].distance = MIN_DISTANCE;
		ctx->contacts[idx].his_timeout = timeout;
		ctx->contacts[idx].timeout = timeout;
		ctx->contacts[idx].last_seen = 0;
	}

	/* Add children to contact list */
	tmp = dir_ptr->node_count;
	dir_ptr = my_node->directions[COMM_GRAPH_CHILDREN];
	for (; idx < tmp + dir_ptr->node_count; idx++) {
		ctx->contacts[idx].node = dir_ptr->nodes[idx - tmp];
		ctx->contacts[idx].distance = MIN_DISTANCE;
		ctx->contacts[idx].his_timeout = timeout;
		ctx->contacts[idx].timeout = timeout;
		ctx->contacts[idx].last_seen = 0;
	}

	/* Add multiroot children to contact list */
	tmp += dir_ptr->node_count;
	dir_ptr = my_node->directions[COMM_GRAPH_MR_CHILDREN];
	for (; idx < tmp + dir_ptr->node_count; idx++) {
		ctx->contacts[idx].node = dir_ptr->nodes[idx - tmp];
		ctx->contacts[idx].distance = MIN_DISTANCE;
		ctx->contacts[idx].his_timeout = timeout;
		ctx->contacts[idx].timeout = timeout;
		ctx->contacts[idx].last_seen = 0;
	}

    return OK;
}

static void tree_validate(tree_context_t *ctx)
{
    unsigned idx;
	for (idx = 0; idx < ctx->contacts_used; idx++) {
		assert((ctx->contacts[idx].timeout == TIMEOUT_NEVER) ||
			   (ctx->contacts[idx].timeout > ctx->contacts[idx].last_seen));
		assert((ctx->contacts[idx].his_timeout == TIMEOUT_NEVER) ||
			   (ctx->contacts[idx].his_timeout > *ctx->step_index + ctx->latency));
	}
}

static tree_distance_t tree_pick_service_distance(tree_context_t *ctx)
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
		each_cycle = TREE_SERVICE_CYCLE_LENGTH(ctx);
		factor = step_index / each_cycle;
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

	/* Look for the contact in the existing list */
	unsigned idx;
	for (idx = 0; idx < ctx->contacts_used; idx++) {
		printf("\nChecking contact #%u/%u (id=%lu)", idx, ctx->contacts_used, id);  // TODO: remove!
		if (ctx->contacts[idx].node == id) {
			*contact = &ctx->contacts[idx];
			if (distance) {
				assert(distance == (*contact)->distance);
			}
			return OK;
		}
	}
	assert(distance != DISTANCE_VACANT);

	/* Contact not found - allocate new! */
	idx = ctx->contacts_used++;
	ctx->contacts = realloc(ctx->contacts,
			ctx->contacts_used * sizeof(tree_contact_t));
	if (!ctx->contacts) {
		return ERROR;
	}

	/* Initialize contact */
	*contact              = &ctx->contacts[idx];
	(*contact)->last_sent = TIMEOUT_NEVER;
	(*contact)->timeout   = TIMEOUT_NEVER;
	(*contact)->distance  = distance;
	(*contact)->node      = id;
	return OK;
}

static inline int tree_next_by_topology(comm_graph_t *graph,
		                                tree_context_t *ctx,
										send_item_t *result)
{
	int ret;
    node_id next_peer;
    unsigned order_idx;
    tree_contact_t *contact;
    step_num current_step_index, timeout;
    unsigned wait_index = ctx->next_wait_index;
    unsigned send_index = ctx->next_send_index;
    comm_graph_node_t *my_node = &graph->nodes[ctx->my_node];

    for (order_idx = ctx->order_indicator;
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
                    	result->dst = next_peer; // For verbose mode
                        return OK;
                    }
                }
            }

            // TODO: wait on (adopted) sons!!! (first wait on subtree - especially if one of the sons dies)

            if (wait_index >= dir_ptr->node_count) {
                wait_index -= dir_ptr->node_count;
            }
            break;

        case TREE_SEND:
            if (send_index < dir_ptr->node_count) {
            	next_peer = dir_ptr->nodes[send_index];
                ret = tree_contact_lookup(ctx, next_peer, DISTANCE_VACANT, &contact);
                if (ret != OK) {
                	return ret;
                }

				/* Send a keep-alive message */
				timeout              = tree_calc_timeout(ctx, contact->distance);
				current_step_index   = *ctx->step_index;
				contact->timeout     = current_step_index + timeout;
				contact->last_sent   = current_step_index;
				contact->his_timeout = TIMEOUT_NEVER;
				result->timeout      = timeout;
                result->msg          = TREE_MSG_DATA(contact->distance);
                result->bitfield     = BITFIELD_FILL_AND_SEND; // TODO: Stop using this value (remove assert too)
                result->dst          = next_peer;
                ctx->next_send_index++;
                return TREE_PACKET_CHOSEN;
            } else {
                send_index -= dir_ptr->node_count;
            }
            break;

        case TREE_WAIT:
            /* Wait for bitmap to be full before distributing */
            if (!IS_FULL_HERE(ctx->my_bitfield)) {
            	result->dst = DESTINATION_UNKNOWN; // For verbose mode
                return OK;
            }
            break;
        }
    }

    /* No more packets to send - we're done here! */
    return DONE;
}

static inline int tree_handle_incoming_packet(tree_context_t *ctx,
		                                      send_item_t *incoming)
{
	enum tree_msg_type msg_type = incoming->msg % TREE_MSG_MAX;
	tree_distance_t msg_distance = incoming->msg / TREE_MSG_MAX;
	tree_contact_t *contact;

	assert(incoming->dst == ctx->my_node);
	int ret = tree_contact_lookup(ctx, incoming->src, msg_distance, &contact);
	if (ret != OK) {
		return ret;
	}

	contact->timeout     = TIMEOUT_NEVER;
	contact->last_seen   = *ctx->step_index;
	contact->his_timeout = tree_calc_timeout(ctx, msg_distance) - ctx->latency;
	if (msg_type == TREE_MSG_KEEPALIVE) {
		/* Prevent keep=alive messages passing data */
		return DONE; // TODO: make this abuse more elegant?
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
            			return ret;
            		}
            	}

                it->distance = DISTANCE_VACANT;
                in_queue->used--;
                return 1;
            }
            used--;
        }
    }
    return 0;
}

static inline int tree_pending_keepalives(tree_context_t *ctx, step_num *eta,
                                          tree_distance_t distance, send_item_t *result)
{
	step_num timeout, current_step_index = *ctx->step_index;
	tree_contact_t *contact, *max_urgency = NULL;
	unsigned idx;

	/* First for incoming keep-alives awaiting acknowledgment */
	for (idx = 0; idx < ctx->contacts_used; idx++) {
		contact = &ctx->contacts[idx];
		if (((contact->distance == distance) || (distance == ANY_DISTANCE)) &&
			(contact->his_timeout != TIMEOUT_NEVER)) {
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
			if (((contact->distance == distance) || (distance == ANY_DISTANCE)) &&
				(contact->his_timeout != TIMEOUT_NEVER)) {
				goto send_keepalive;
			}
		}
	}

	return OK;

send_keepalive:
	/* Send a keep-alive message */
	timeout              = tree_calc_timeout(ctx, contact->distance);
	contact->timeout     = current_step_index + timeout;
	contact->last_sent   = current_step_index;
	contact->his_timeout = TIMEOUT_NEVER;
	result->bitfield     = BITFIELD_FILL_AND_SEND;
	result->msg          = TREE_MSG_KA(contact->distance);
	result->dst          = contact->node;
	result->timeout      = timeout;
	return TREE_PACKET_CHOSEN;
}

int tree_next(comm_graph_t *graph, send_list_t *in_queue,
		      tree_context_t *ctx, send_item_t *result)
{
	tree_distance_t distance;
	int ret;

	/* Step #0: Assert algorithm's assumptions */
	// TODO: tree_validate(ctx);

	/* Step #1: If data can be sent - send it! (w/o reading incoming messages) */
	ret = tree_next_by_topology(graph, ctx, result);
	if (ret == TREE_PACKET_CHOSEN) {
		assert(result->bitfield == BITFIELD_FILL_AND_SEND);
		return OK;
	}
	if (ret != OK) {
		return ret; /* Typically "DONE" */
	}

	/* Step #2: Determine which tree-distance gets service this time */
	distance = tree_pick_service_distance(ctx);

service_distance:
	printf("\nNode #%lu is serving distance %u", ctx->my_node, distance);  // TODO: remove!
	/* Step #3: Data (from service-distance) comes first, then keep-alives */
	if (queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_DATA, distance, result) ||
		queue_get_msg_by_distance(ctx, in_queue, TREE_MSG_KEEPALIVE, distance, result)) {
		assert(result->src != ctx->my_node);
		return OK;
	}

	/* Step #4: If its time is up - send a keep-alive within service-distance */
	ret = tree_pending_keepalives(ctx, graph->nodes[ctx->my_node].data_eta, distance, result);
	if (ret == TREE_PACKET_CHOSEN) {
		assert(result->bitfield == BITFIELD_FILL_AND_SEND);
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
    return OK;
}

int tree_fix(comm_graph_t *graph, tree_context_t *ctx,
			 tree_recovery_method_t recovery, node_id broken)
{
    /* Exclude the broken node from further sends */
    int ret = comm_graph_append(graph, ctx->my_node, broken, COMM_GRAPH_EXCLUDE);
    if (ret != OK) {
        return ret;
    }

    printf("\nOMIT: %lu omits %lu", ctx->my_node, broken); // TODO: remove!

    switch (recovery) {
    case COLLECTIVE_RECOVERY_CATCH_THE_BUS: // TODO: implement!
    case COLLECTIVE_RECOVERY_BROTHER_FIRST: //TODO: implement!
        /* calc brother - reverse BFS */
        /* add brother as father, myself as his child */
    case COLLECTIVE_RECOVERY_FATHER_FIRST:
        if (broken < ctx->my_node) {
            /* the broken node is above me in the tree: */
            if (comm_graph_count(graph, broken, COMM_GRAPH_FATHERS) == 0) {
                /* if broken has no fathers - add his children to COMM_GRAPH_EXTRA_FATHERS and me as their son */
                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
                if (ret != OK) {
                    return ret;
                }

                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_MR_CHILDREN, COMM_GRAPH_EXTRA_FATHERS);
            } else {
                /* add broken's fathers to COMM_GRAPH_EXTRA_FATHERS, and me as their son */
                ret = comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_FATHERS, COMM_GRAPH_EXTRA_FATHERS);
            }
            if (ret != OK) {
                return ret;
            }
        }
        return comm_graph_copy(graph, broken, ctx->my_node, COMM_GRAPH_CHILDREN, COMM_GRAPH_EXTRA_CHILDREN);
        break;


    case COLLECTIVE_RECOVERY_ALL:
        break;
    }
    return ERROR;
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
                    ret = comm_graph_append(*graph, next_father, next_child, COMM_GRAPH_MR_CHILDREN);
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
    	node_id child_count;

    	/* Find the child arriving last */
    	dir = (*graph)->nodes[first_child].directions[COMM_GRAPH_CHILDREN];
    	child_count = dir->node_count;
    	for (next_child = 0; next_child < dir->node_count; next_child++) {
    		child_eta = (*graph)->nodes[dir->nodes[next_child]].data_eta[DATA_ETA_SUBTREE];
    		if (child_eta > eta) {
    			eta = child_eta;
    		}
    	}

    	/* Set the ETA for all the children */
    	(*graph)->nodes[first_child].data_eta[DATA_ETA_SUBTREE] =
    			child_count ? eta + spec->latency + 1 : 0;
    }


    if (is_multiroot) {
    	step_num child_eta, eta = 0;
    	comm_graph_direction_ptr_t dir;

    	/* Find the "multi-root" child (for a root - other roots) arriving last */
		for (first_child = 0; first_child < tree_radix; first_child++) {
	    	dir = (*graph)->nodes[first_child].directions[COMM_GRAPH_MR_CHILDREN];
	    	child_count = dir->node_count;
	    	for (next_child = 0; next_child < dir->node_count; next_child++) {
	    		child_eta = (*graph)->nodes[dir->nodes[next_child]].data_eta[DATA_ETA_SUBTREE];
	    		if (child_eta > eta) {
	    			eta = child_eta;
	    		}
	    	}
		}

		/* Set the ETA for all the roots */
		eta += spec->latency + 1;
		for (first_child = 0; first_child < tree_radix; first_child++) {
			(*graph)->nodes[first_child].data_eta[DATA_ETA_FULL_TREE] = (eta++);
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
