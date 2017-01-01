/*
 * comm_graph.c
 *
 *  Created on: Dec 19, 2016
 *      Author: alexma
 */

#include <stdlib.h>
#include <string.h>

#define OK (0)
#define ERROR (-1)

typedef unsigned long node_id;

enum comm_graph_direction_count {
	COMM_GRAPH_FLOW = 1, /* Data flows in one direction, e.g. recursive doubling */
	COMM_GRAPH_BIDI = 2, /* Bidirectional data flow, e.g. up and down a tree */

	COMM_GRAPH_MAX_DIMENTIONS = 2
};

typedef struct comm_graph_direction {
	node_id node_count;
	node_id arr_length;
	node_id nodes[0];
} comm_graph_direction_t;

typedef struct comm_graph_node {
	enum comm_graph_direction_count direction_count;
	comm_graph_direction_t *directions[COMM_GRAPH_MAX_DIMENTIONS];
} comm_graph_node_t;

typedef struct comm_graph {
	enum comm_graph_direction_count direction_count;
	node_id node_count;
	comm_graph_node_t nodes[0];
} comm_graph_t;

comm_graph_t* comm_graph_create(unsigned long node_count,
		enum comm_graph_direction_count direction_count);

void comm_graph_destroy(comm_graph_t* comm_graph);

int comm_graph_append(comm_graph_t* comm_graph, node_id father, node_id child);
