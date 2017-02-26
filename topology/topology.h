#include "comm_graph.h"

#ifdef __linux__
#define CYCLIC_RANDOM(spec, mod) (rand_r(&(spec)->topology.random.random_seed) % (mod))
#define FLOAT_RANDOM(spec) ((rand_r(&(spec)->topology.random.random_seed)) / ((float)RAND_MAX))
#elif _WIN32
#define CYCLIC_RANDOM(spec, mod) (rand() % (mod))
#define FLOAT_RANDOM(spec) (((float)rand()) / RAND_MAX)
#else
#error "OS not supported!"
#endif

typedef enum topology_type
{
    COLLECTIVE_TOPOLOGY_NARRAY_TREE = 0,
    COLLECTIVE_TOPOLOGY_KNOMIAL_TREE,
    COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE,
    COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE,
    COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING,
    COLLECTIVE_TOPOLOGY_RANDOM_PURE,
    COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST, /* One const step for every <radix - 2> random steps */
    COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM, /* One random step for every <radix - 1> const steps */
    COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR, /* After every <radix> steps - add one const step to the cycle */
    COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL, /* After every <radix> steps - double the non-random steps in the cycle */
    COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC, /* Send to missing nodes from bitfield, the 50:50 random hybrid*/

    COLLECTIVE_TOPOLOGY_ALL /* default, must be last */
} topology_type_t;

typedef enum model_type
{
    COLLECTIVE_MODEL_ITERATIVE = 0, /* Basic collective */
    COLLECTIVE_MODEL_PACKET_DELAY,  /* Random packet delay */
    COLLECTIVE_MODEL_PACKET_DROP,   /* Random failure at times */
    COLLECTIVE_MODEL_TIME_OFFSET,   /* Random start time offset */
	// TODO: COLLECTIVE_MODEL_NODE_FAILURE,  /* Node stops functioning */

    COLLECTIVE_MODEL_ALL /* default, must be last */
} model_type_t;

typedef struct topology_spec
{
	int verbose;
	node_id my_rank;
	node_id node_count;
	node_id local_node_count;
	unsigned char *my_bitfield;

	unsigned step_index; /* struct abuse in favor of optimization */

	topology_type_t topology_type;
	union {
		struct {
		    unsigned radix;
		} tree;
		struct {
		    unsigned radix;
		} butterfly;
		struct {
			unsigned cycle_random;
			unsigned cycle_const;
			unsigned random_seed;
		} random;
	} topology;

	model_type_t model_type;
	union {
		float packet_drop_rate;
		unsigned time_offset_max;
		unsigned packet_delay_max;
	} model;
} topology_spec_t;

enum topology_map_slot
{
	TREE,
	BUTTERFLY,
	RANDOM
};

typedef struct topo_funcs
{
	int (*build_f)(topology_spec_t *spec, comm_graph_t **graph);
	int (*start_f)(topology_spec_t *spec, comm_graph_t *graph, void **internal_ctx);
	int (*next_f)(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
	int (*fix_f)(comm_graph_t *graph, void *internal_ctx, node_id broken);
	int (*end_f)(void *internal_ctx);
} topo_funcs_t;

typedef struct topology_iterator {
	comm_graph_t *graph;
	topology_spec_t *spec;
	void *ctx; /* internal context for each iterator, type depends on topology */
	topo_funcs_t funcs;
	unsigned time_offset;
	unsigned random_seed;
} topology_iterator_t;

int topology_iterator_create(topology_spec_t *spec, topology_iterator_t *iterator);

#define NO_PACKET ((unsigned)-1) /* set as distance */

int topology_iterator_next(topology_iterator_t *iterator, node_id *target, unsigned *distance);

int topology_iterator_omit(topology_iterator_t *iterator, node_id broken);

void topology_iterator_destroy(topology_iterator_t *iterator);

//int topology_test(collective_topology_t topology, node_id node_count);
