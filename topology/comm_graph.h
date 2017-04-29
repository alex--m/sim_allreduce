#include <stdlib.h>
#include <string.h>


#include <stdio.h> // printf only

#define OK (0)
//#define ERROR (-1)
#define DONE (1)
#define ERROR (int)(printf("Internal error at %s, line %d.\n",__FILE__, __LINE__) && (-1))

typedef unsigned long node_id;
typedef unsigned group_id; /* each group simulates multiple nodes */

enum comm_graph_direction_type {
	COMM_GRAPH_CHILDREN = 0,  /* Children in a tree, or just next nodes in flow-like graphs */
	COMM_GRAPH_FATHERS,       /* Fathers in a tree */

	COMM_GRAPH_EXCLUDE,       /* Nodes to avoid during fault-tolerance*/
	COMM_GRAPH_EXTRA_CHILDREN,/* New sons, results of fault-tolerance */
	COMM_GRAPH_EXTRA_FATHERS, /* New fathers, results of fault-tolerance */

	COMM_GRAPH_MAX_DIMENTIONS /* MUST BE LAST */
};

typedef struct comm_graph_direction {
	node_id node_count;
	node_id arr_length;
	node_id nodes[1]; /* can grow more - variable-length structure */
} comm_graph_direction_t;

typedef comm_graph_direction_t *comm_graph_direction_ptr_t;

typedef struct comm_graph_node {
	comm_graph_direction_ptr_t directions[COMM_GRAPH_MAX_DIMENTIONS];
} comm_graph_node_t;

typedef struct comm_graph {
	int is_bidirectional;
	node_id node_count;
	comm_graph_node_t nodes[0];
} comm_graph_t;

comm_graph_t* comm_graph_create(unsigned long node_count, int is_bidirectional);

comm_graph_t* comm_graph_clone(comm_graph_t* original);

void comm_graph_destroy(comm_graph_t* comm_graph);

int comm_graph_append(comm_graph_t* comm_graph, node_id src, node_id dst,
		enum comm_graph_direction_type direction);

int comm_graph_count(comm_graph_t* comm_graph, node_id id,
		enum comm_graph_direction_type direction);

int comm_graph_copy(comm_graph_t* comm_graph, node_id src, node_id dst,
		enum comm_graph_direction_type src_direction,
		enum comm_graph_direction_type dst_direction);

void comm_graph_print(comm_graph_t* comm_graph);
