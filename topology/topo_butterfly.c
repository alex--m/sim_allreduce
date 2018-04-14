#include "../state/state_matrix.h"
#include "topology.h"

struct butterfly_ctx {
    comm_graph_direction_t *my_peers;
    unsigned char *my_bitfield;
    unsigned next_child_index;
    unsigned check_interval;
    node_id extra_count; /* if non power of radix */
#define EXTRA_IS_ME ((node_id)-1)
};


enum butterfly_msg_type {
    BUTTERFLY_MSG_DATA,
    BUTTERFLY_MSG_KEEPALIVE,
    BUTTERFLY_MSG_ALIVE,
    BUTTERFLY_MSG_LAST
};


static inline unsigned get_closest_power(node_id node_count, unsigned radix)
{
    unsigned power = 1;
    while (power * radix <= node_count) {
        power *= radix;
    }
    return power;
}

size_t butterfly_ctx_size() {
    return sizeof(struct butterfly_ctx);
}

int butterfly_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct butterfly_ctx *internal_ctx)
{
    unsigned radix = spec->topology.butterfly.radix;
    unsigned power = get_closest_power(spec->node_count, radix);

    internal_ctx->check_interval = radix - 1;
    internal_ctx->next_child_index = 0;
    internal_ctx->my_bitfield = spec->my_bitfield;
    internal_ctx->my_peers = graph->nodes[spec->my_rank].directions[COMM_GRAPH_CHILDREN];

    if (spec->node_count != power) {
        if (spec->my_rank < power) {
            unsigned total_extra_sons = (spec->node_count - power);
            unsigned extra_sons_per_rank = total_extra_sons / power;
            unsigned additional_extra_sons = total_extra_sons % power;
            internal_ctx->extra_count = extra_sons_per_rank +
                    (spec->my_rank < additional_extra_sons);
        } else {
            internal_ctx->extra_count = EXTRA_IS_ME;
        }
    } else {
        internal_ctx->extra_count = 0;
    }
    return OK;
}

void butterfly_stop(struct butterfly_ctx *internal_ctx)
{
	return;
}

int butterfly_next(comm_graph_t *graph, send_list_t *in_queue,
                   struct butterfly_ctx *internal_ctx, send_item_t *result)
{
    int offset;
    node_id next_peer;

    /* Check if I'm the extra */
    if (internal_ctx->extra_count == EXTRA_IS_ME) {
        /* Send to my father */
        if (internal_ctx->next_child_index < internal_ctx->my_peers->node_count) {
            next_peer = internal_ctx->next_child_index++;
            next_peer = internal_ctx->my_peers->nodes[next_peer];
            result->msg = BUTTERFLY_MSG_DATA;
            result->dst = next_peer;
            return OK;
        }

        /* Expect results */
        while (internal_ctx->next_child_index < 2 * internal_ctx->my_peers->node_count) {
            next_peer = internal_ctx->next_child_index;
            next_peer -= internal_ctx->my_peers->node_count;
            next_peer = internal_ctx->my_peers->nodes[next_peer];
            if (IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
                internal_ctx->next_child_index++;
            } else {
                /* Waiting for my "father" */
                result->dst = next_peer;
                goto process_incoming;
            }
        }
        return DONE;
    }

    /* Check on "extra sons" */
    if (internal_ctx->extra_count) {
        /* Check incoming data from "extra sons" */
        while (internal_ctx->next_child_index < internal_ctx->extra_count) {
            next_peer = internal_ctx->next_child_index;
            next_peer = internal_ctx->my_peers->nodes[next_peer];
            if (IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
                internal_ctx->next_child_index++;
            } else {
                result->dst = next_peer;
                goto process_incoming;
            }
        }
    }

    /* Wait for this level before ascending to the next level */
    if ((internal_ctx->next_child_index >= internal_ctx->extra_count + internal_ctx->check_interval) &&
        (internal_ctx->next_child_index <= internal_ctx->my_peers->node_count) &&
        (((internal_ctx->next_child_index - internal_ctx->extra_count) % internal_ctx->check_interval) == 0)) {
        for (offset = internal_ctx->check_interval;
             offset > 0;
             offset--) {
            next_peer = internal_ctx->next_child_index - offset;
            next_peer = internal_ctx->my_peers->nodes[next_peer];
            if (!IS_BIT_SET_HERE(next_peer, internal_ctx->my_bitfield)) {
                result->dst = next_peer;
                goto process_incoming;
            }
        }
    }

    /* Check for completion */
    if (internal_ctx->next_child_index >= internal_ctx->my_peers->node_count +
            internal_ctx->extra_count) {
        return DONE;
    }

    /* Send to the next target on the list */
    if (internal_ctx->next_child_index < internal_ctx->my_peers->node_count) {
        next_peer = internal_ctx->next_child_index++;
        result->dst = internal_ctx->my_peers->nodes[next_peer];
        result->msg = BUTTERFLY_MSG_DATA;
        return OK;
    }

    /* Send to "extra sons" */
    if (internal_ctx->next_child_index < internal_ctx->extra_count +
            internal_ctx->my_peers->node_count) {
        next_peer = internal_ctx->next_child_index++;
        next_peer -= internal_ctx->my_peers->node_count;
        result->dst = internal_ctx->my_peers->nodes[next_peer];
        result->msg = BUTTERFLY_MSG_DATA;
        return OK;
    }

    return DONE;

process_incoming:
    //if (queue_check_msg(in_queue, BUTTERFLY_MSG_LAST, result)) {
    //    return OK;
    //}

// TODO: send keep-alives!
    result->distance = DISTANCE_NO_PACKET;
    return OK;
}

int butterfly_fix(comm_graph_t *graph, void *internal_ctx,
		          tree_recovery_method_t recovery, node_id broken)
{
    return ERROR;
}

int butterfly_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * Recursive K-ing (tree_radix=k):
     * For every level X - divide the ranks into groups of size k^(X-1),
     * assign each rank with the its relative index in that group, the divide
     * the ranks into groups of size k^X, and send to all members of the new
     * group assigned the same index as you.
     * If the last group (of size Y) is incomplete (Y < X): senders within
     * this group of relative index exceeding Y will send to the index modulo Y.
     *
     * Graphic example :
     * http://www.mcs.anl.gov/~thakur/papers/ijhpca-coll.pdf (for recursive doubling)
     * http://dl.acm.org/citation.cfm?id=2402483 (Reindexed Recursive K-ing)
     */

    unsigned node_count = spec->node_count;
    unsigned radix = spec->topology.butterfly.radix;
    node_id max_group_size;
    node_id group_size;
    node_id jump_size;
    node_id next_id;
    node_id jump;

    *graph = comm_graph_create(node_count, 0);
    if (!*graph) {
        return ERROR;
    }

    /* Set excess nodes (non power of radix) to check in */
    max_group_size = get_closest_power(node_count, radix);
    for (next_id = max_group_size; next_id < node_count; next_id++) {
        comm_graph_append(*graph, next_id, next_id % max_group_size,
                COMM_GRAPH_CHILDREN);
        comm_graph_append(*graph, next_id % max_group_size, next_id,
                COMM_GRAPH_CHILDREN);
    }

    for (jump_size = 1, group_size = radix;
         jump_size < max_group_size;
         jump_size = group_size, group_size *= radix) {
        for (next_id = 0; next_id < max_group_size; next_id++) {
            for (jump = 1; jump < radix; jump++) {
                node_id group_start = next_id - (next_id % group_size);
                node_id next_son = group_start + (((next_id - group_start) +
                        (jump_size * jump)) % group_size);
                comm_graph_append(*graph, next_id, next_son, COMM_GRAPH_CHILDREN);
            }
        }
    }

    return OK;
}
