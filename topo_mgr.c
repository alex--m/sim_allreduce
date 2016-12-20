/*
 * recursive_doubling.c
 *
 *  Created on: Dec 19, 2016
 *      Author: alexma
 */

#include "comm_graph.h"

//int comm_graph_append(comm_graph_t* comm_graph, node_id father, node_id child);

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;

int topology_create(collective_topology_t topology, node_id node_count)
{
	assert(!current_topology);
	assert(!current_reference_count);


	switch (topology) {
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

	case COLLECTIVE_TOPOLOGY_RANDOM:
	case COLLECTIVE_TOPOLOGY_RANDOM_ENHANCED:
	}

	current_topology = comm_graph_create(node_count, direction_count);
	if (!current_topology) {
		return -1;
	}

	current_reference_count++;
	return 0;
}

void topology_destroy() {
	if (!--current_reference_count) {
		comm_graph_destroy(current_topology);
		current_topology = NULL;
	}
}
