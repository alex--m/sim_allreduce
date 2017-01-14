#include "topology.h"

static comm_graph_t* build_tree(unsigned node_count,
							    unsigned tree_radix,
								int is_knomial,
								int is_multiroot)
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

	node_id child_count;
	node_id next_child;
	node_id next_father;
	node_id first_child = 1;
	node_id first_father = 0;

	comm_graph_t* tree = comm_graph_create(node_count, COMM_GRAPH_BIDI);
	if (!tree) {
		return NULL;
	}

	if (tree_radix > node_count) {
		tree_radix = node_count;
	}

	if (is_multiroot) {
		for (next_father = 0; next_father < tree_radix; next_father++) {
			for (next_child = 0; next_child < tree_radix; next_child++) {
				if (next_father != next_child) {
					if (comm_graph_append(tree, next_father, next_child)) {
						comm_graph_destroy(tree);
						return NULL;
					}
				}
			}
		}
		first_child = tree_radix;
	}

	while (first_child < node_count) {
		for (child_count = 0; child_count < tree_radix; child_count++) {
			for (next_father = first_father, next_child = first_child;
				 (next_father < first_child) && (next_child < node_count);
				 next_father++, next_child++) {
				if (comm_graph_append(tree, next_father, next_child)) {
					comm_graph_destroy(tree);
					return NULL;
				}
			}
		}

		first_child += (first_child - first_father) * tree_radix;
		if (!is_knomial) {
			first_father = next_father;
		}
	}

	return tree;
}

int tree_build(topology_spec_t spec, comm_graph_t **graph)
{
	enum comm_graph_direction_count direction_count;

	switch (spec->topology) {
	case COLLECTIVE_TOPOLOGY_NARRAY_TREE:
		direction_count = COMM_GRAPH_BIDI;
		break;

	case COLLECTIVE_TOPOLOGY_KNOMIAL_TREE:
		direction_count = COMM_GRAPH_BIDI;
		break;

	case COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE:
		direction_count = COMM_GRAPH_BIDI;
		break;

	case COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE:
		direction_count = COMM_GRAPH_BIDI;
		break;
	}

	*graph = comm_graph_create(spec->node_count, direction_count);
	return (*graph != NULL) ? OK : ERROR;
}

int fix_tree(comm_graph_t* tree, node_id bad_node) {
/*
 * On the way up, if a node is broken: send to his father, then try his peers one after the other, then the grandfather, then the father's peers....
 * On the way down - send to his sons...
 */
}
