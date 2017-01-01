/*
 * recursive_doubling.c
 *
 *  Created on: Dec 19, 2016
 *      Author: alexma
 */

#include "comm_graph.h"
#include "sim_allreduce.h"

#include <assert.h>

struct topology_iterator {
	comm_graph_t *graph;
	comm_graph_node_t my_node;
};

comm_graph_t *current_topology = NULL;
unsigned current_reference_count = 0;

int topology_iterator_create(collective_topology_t topology, node_id node_count, topology_iterator_t *iterator)
{
	unsigned direction_count;

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
	}

	current_topology = comm_graph_create(node_count, direction_count);
	if (!current_topology) {
		return -1;
	}

	current_reference_count++;
	return 0;
}

node_id topology_iterator_next(topology_iterator_t *iterator)
{

}

int topology_iterator_omit(topology_iterator_t *iterator, node_id broken)
{
	if (iterator->graph == current_topology) {
		iterator->graph = comm_graph_clone(current_topology);
		topology_destroy(current_topology);
	}

	fix(iterator->graph, broken);
}

void topology_destroy() {
	if (!--current_reference_count) {
		comm_graph_destroy(current_topology);
		current_topology = NULL;
	}
}
