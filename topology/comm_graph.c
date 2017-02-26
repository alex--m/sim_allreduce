#include <assert.h>
#include "comm_graph.h"

#define COMM_GRAPH_DIRECTION_APPEND(node, direction, node_id) { \
	if (node->directions[direction]->node_count == node->directions[direction]->arr_length) { \
		void *tmp; \
		node->directions[direction]->arr_length *= 2; \
		tmp = realloc(node->directions[direction], sizeof(comm_graph_direction_t) \
				+ (node->directions[direction]->arr_length * sizeof(node_id))); \
		if (!tmp) { \
			return -1; \
		} \
		node->directions[direction] = tmp; \
	} \
	node->directions[direction]->nodes[node->directions[direction]->node_count++] = node_id; \
}\

comm_graph_t* comm_graph_create(node_id node_count,
		enum comm_graph_direction_count direction_count)
{
	comm_graph_node_t *new_node;
	unsigned node_index, direction_index;

	size_t size = sizeof(comm_graph_t) + node_count * sizeof(comm_graph_node_t);
	comm_graph_t *res = calloc(1, size);
	if (!res) {
		return NULL;
	}

	for (node_index = 0, new_node = res->nodes;
		 node_index < node_count;
		 node_index++, new_node++) {
		new_node->direction_count = direction_count;

		for (direction_index = 0;
			 direction_index < direction_count;
			 direction_index++) {
			new_node->directions[direction_index] = malloc(sizeof(comm_graph_direction_t));
			if (!new_node->directions[direction_index]) {
				return NULL;
			}

			new_node->directions[direction_index]->arr_length = 1;
			new_node->directions[direction_index]->node_count = 0;
		}
	}

	res->node_count = node_count;
	res->direction_count = direction_count;
	return res;
}

comm_graph_t* comm_graph_clone(comm_graph_t* original)
{
	comm_graph_node_t *node, *new_node;
	unsigned node_index, direction_index;

	comm_graph_t *res = comm_graph_create(original->node_count,
			original->direction_count);
	if (!res) {
		return NULL;
	}

	for (node_index = 0, new_node = res->nodes, node = original->nodes;
		 node_index < original->node_count;
		 node_index++, new_node++, node++) {
		new_node->direction_count = node->direction_count;

		for (direction_index = 0;
			 direction_index < node->direction_count;
			 direction_index++) {
			comm_graph_direction_t *direction, *new_direction;
			direction = node->directions[direction_index];

			size_t direction_size = sizeof(comm_graph_direction_t)
					+ direction->arr_length * sizeof(node_id);
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
	unsigned node_index, direction_index;

	for (node_index = 0, node = comm_graph->nodes;
		 node_index < comm_graph->node_count;
		 node_index++, node++) {

		for (direction_index = 0;
			 direction_index < node->direction_count;
			 direction_index++) {
			comm_graph_direction_t *direction =
				node->directions[direction_index];

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
	assert(father < comm_graph->node_count);
	assert(child < comm_graph->node_count);
	COMM_GRAPH_DIRECTION_APPEND(node, 0, child);

	if (comm_graph->direction_count == COMM_GRAPH_BIDI) {
		node = &comm_graph->nodes[child];
		COMM_GRAPH_DIRECTION_APPEND(node, 1, father);
	}

	return OK;
}

static void recursive_print(comm_graph_t* comm_graph, node_id node, int level)
{
	node_id iterator;
	comm_graph_direction_ptr_t next = comm_graph->nodes[node].directions[0];
	for (iterator = 0; iterator < next->node_count; iterator++) {
		if (next->nodes[iterator] > node) {
			recursive_print(comm_graph, next->nodes[iterator], level + 1);
		}
	}

	for (iterator = 0; iterator < level; iterator++) {
		printf("    ");
	}
	printf("%lu\n", node);
}

void comm_graph_print(comm_graph_t* comm_graph)
{
	recursive_print(comm_graph, 0, 0);
}
