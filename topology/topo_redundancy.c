#include "../state/state_matrix.h"
#include "topology.h"

struct redundancy_ctx {
    comm_graph_direction_t *my_peers;
    unsigned char *my_bitfield;
    unsigned next_index;
    node_id my_rank;
    node_id origin;
#define ORIGIN_TBD ((node_id)-1)
};

size_t redundancy_ctx_size() {
    return sizeof(struct redundancy_ctx);
}

int redundancy_start(topology_spec_t *spec, comm_graph_t *graph,
                    struct redundancy_ctx *internal_ctx)
{
    internal_ctx->next_index  = 0;
    internal_ctx->my_bitfield = spec->my_bitfield;
    internal_ctx->my_peers    = graph->nodes[spec->my_rank].directions[COMM_GRAPH_CHILDREN];
    internal_ctx->my_rank     = spec->my_rank;
    internal_ctx->origin      = ORIGIN_TBD;
    return OK;
}

void redundancy_stop(struct redundancy_ctx *internal_ctx)
{
	free(internal_ctx);
}

int redundancy_next(comm_graph_t *graph, send_list_t *in_queue,
                    struct redundancy_ctx *internal_ctx, send_item_t *result)
{
    /* Wait for the first data message */
    if (internal_ctx->origin == ORIGIN_TBD) {
        /* check incoming messages */
        if (in_queue->used) {
            send_item_t *it;
            unsigned pkt_idx, used;
            for (pkt_idx = 0, it = in_queue->items;
                 pkt_idx < in_queue->allocated;
                 pkt_idx++, it++) {
                if (it->distance != DISTANCE_VACANT) {
                    internal_ctx->origin = 0;
                    memcpy(result, it, sizeof(send_item_t));
                    it->distance = DISTANCE_VACANT;
                    in_queue->used--;
                    return OK;
                }
            }
        }

        result->distance = DISTANCE_NO_PACKET;
        return OK;
    }

    if (internal_ctx->next_index == internal_ctx->my_peers->node_count) {
        // TODO: drain queue?
        return DONE;
    }

    /* Send to all my peers (except the origin of the data) */
    node_id next_peer = internal_ctx->my_peers->nodes[internal_ctx->next_index++];
    if (next_peer == internal_ctx->origin) {
        next_peer = internal_ctx->my_peers->nodes[internal_ctx->next_index++];
    }

    result->msg      = 0; /* Anything would do except MSG_DEATH */
    result->dst      = next_peer;
    result->src      = internal_ctx->my_rank;
    result->bitfield = internal_ctx->my_bitfield;
    return OK;
}

int redundancy_fix(comm_graph_t *graph, void *internal_ctx,
		          tree_recovery_method_t recovery, node_id broken)
{
    return OK;
}

int debruijn_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * De Bruijn Networks
     * For every node X, with a binary representation of x[0],x[1],...,x[N] -
     * connections include all nodes of the following binary representation:
     * 1. 1,x[0],x[1],...,x[N-1]
     * 2. 0,x[0],x[1],...,x[N-1]
     * 3. x[1],x[2],...,x[N],1
     * 4. x[1],x[2],...,x[N],0
     *
     * Graphic example :
     * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.28.1799&rep=rep1&type=pdf
     */
    node_id node_count = spec->node_count;
    node_id next_id    = node_count - 1;
    unsigned bit_count = 8 * sizeof(next_id) - __builtin_ctzl(next_id);
    node_id left_bit   = 1 << (bit_count - 1);
    node_id extra_bit  = 1 << bit_count;

    *graph = comm_graph_create(node_count, 0);
    if (!*graph) {
        return ERROR;
    }

    for (next_id = 0; next_id < node_count; next_id++) {
        node_id connection = (next_id >> 1) |  left_bit;
        if ((connection != next_id) && (connection < node_count)) {
            comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
        }

        connection = (next_id >> 1) & ~left_bit;
        if ((connection != next_id) && (connection < node_count)) {
            comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
        }

        connection =  ((next_id << 1) | 1) & ~extra_bit;
        if ((connection != next_id) && (connection < node_count)) {
            comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
        }

        connection = (next_id << 1) & ~1 & ~extra_bit;
        if ((connection != next_id) && (connection < node_count)) {
            comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
        }
    }

    return OK;
}

int hypercube_build(topology_spec_t *spec, comm_graph_t **graph)
{
    /*
     * Hypercube
     * For every node X, with a binary representation of x[0],x[1],...,x[N] -
     * connections include all nodes of the following binary representation:
     * 1. x[0],x[1],...,x[k],0,x[k+2],x[k+3],...,x[N]
     * 2. x[0],x[1],...,x[k],1,x[k+2],x[k+3],...,x[N]
     *
     * Graphic example :
     * http://pages.cs.wisc.edu/~tvrdik/5/html/Section5.html
     */
    node_id node_count = spec->node_count;
    node_id next_id    = node_count - 1;
    unsigned bit_count = 8 * sizeof(next_id) - __builtin_ctzl(next_id);
    unsigned next_bit;

    *graph = comm_graph_create(node_count, 0);
    if (!*graph) {
        return ERROR;
    }

    for (next_id = 0; next_id < node_count; next_id++) {
        for (next_bit = 0; next_bit < bit_count; next_bit++) {
            node_id connection = next_id | (1 << next_bit);
            if ((connection != next_id) && (connection < node_count)) {
                comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
            }

            connection = next_id & ~(1 << next_bit);
            if ((connection != next_id) && (connection < node_count)) {
                comm_graph_append(*graph, next_id, connection, COMM_GRAPH_CHILDREN);
            }
        }
    }

    return OK;
}

