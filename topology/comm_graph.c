/*
 * comm_graph.c
 *
 *  Created on: Dec 19, 2016
 *      Author: alexma
 */

#include "comm_graph.h"

#define COMM_GRAPH_DIRECTION_APPEND(node, direction, node_id) { \
	if (node->directions[direction]->node_count == node->directions[direction]->arr_length) { \
		void *tmp; \
		node->directions[direction]->arr_length *= 2; \
		tmp = realloc(node->directions[direction], \
				sizeof(node_id) * node->directions[direction]->arr_length); \
		if (!tmp) { \
			return -1; \
		} \
	} \
	node->directions[direction]->nodes[node->directions[direction]->node_count++] = node_id; \
}\

comm_graph_t* comm_graph_create(node_id node_count,
		enum comm_graph_direction_count direction_count)
{
	size_t size = sizeof(comm_graph_t) + node_count * sizeof(comm_graph_node_t);
	comm_graph_t *res = calloc(1, size);
	if (!res) {
		return NULL;
	}

	res->node_count = node_count;
	res->direction_count = direction_count;
	return res;
}

comm_graph_t* comm_graph_clone(comm_graph_t* original)
{
	comm_graph_node_t* node, *new_node;
	comm_graph_direction_t *direction, *new_direction;
	unsigned node_index, direction_index;

	comm_graph_t *res = comm_graph_create(original->node_count,
			original->direction_count);
	if (!res) {
		return NULL;
	}

	for (node_index = 0, new_node = res->nodes, node = original->nodes;
		 node_index < original->node_count;
		 node_index++, new_node++, node++) {

		for (direction_index = 0, direction = node->directions;
			 direction_index < node->direction_count;
			 direction_index++, direction++) {

			size_t direction_size = sizeof(*direction) + direction->arr_length * sizeof(node_id);
			new_direction = malloc(direction_size);

			if (!new_direction) {
				comm_graph_destroy(res);
				return NULL;
			}

			memcpy(new_direction, direction, direction_size);
			new_node->directions[direction_index] = new_direction;
		}
	}

	return res;
}

void comm_graph_destroy(comm_graph_t* comm_graph)
{
	comm_graph_node_t* node;
	comm_graph_direction_t *direction;
	unsigned node_index, direction_index;

	for (node_index = 0, node = comm_graph->nodes;
		 node_index < comm_graph->node_count;
		 node_index++, node++) {

		for (direction_index = 0, direction = node->directions;
			 direction_index < node->direction_count;
			 direction_index++, direction++) {

			if (direction) {
				free(direction);
			}
		}
	}
	free(comm_graph);
}

int comm_graph_append(comm_graph_t* comm_graph, node_id father, node_id child)
{
	comm_graph_node_t *node = &comm_graph->nodes[father];
	COMM_GRAPH_DIRECTION_APPEND(node, 0, child);

	if (comm_graph->direction_count == COMM_GRAPH_BIDI) {
		node = &comm_graph->nodes[child];
		COMM_GRAPH_DIRECTION_APPEND(node, 1, father);
	}
}
