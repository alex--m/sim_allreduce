#include "comm_graph.h"

comm_graph_t* build_recursive(unsigned node_count, unsigned radix)
{
	/*
}
int send_recursive_k_ing(
        unsigned host_list_size,
        unsigned my_index_in_list,
        unsigned tree_radix,
        int is_multiroot,
        exchange_f target_rank_cb,
        collective_iteration_ctx_t *ctx)
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

	node_id next_id = 0;
	node_id jump_size = 1;
	node_id group_size = radix;

	comm_graph_t* flow = comm_graph_create(node_count, COMM_GRAPH_FLOW);
	if (!tree) {
		return NULL;
	}

	if (radix > node_count) {
		radix = node_count;
	}

	while (jump_size ) {
		node_id jump;
		for (next_id = 0; next_id < node_count; next_id++) {
			for (jump = 0; jump < radix - 1; jump++) {
				node_id group_start = next_id - (next_id % group_size);
				comm_graph_append(flow, next_id, group_start + ((jump_size * jump) % group_size));
			}
		}
		jump_size = group_size;
		group_size *= radix;
	}

	return flow;
}

int fix_recursive(comm_graph_t* tree, node_id bad_node) {
    return -1;
}
