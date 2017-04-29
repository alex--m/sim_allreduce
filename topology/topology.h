#include "comm_graph.h"

#ifdef __linux__
#define CYCLIC_RANDOM(spec, mod) (rand_r(&(spec)->random_seed) % (mod))
#define FLOAT_RANDOM(spec) (((float)rand_r(&(spec)->random_seed)) / ((float)RAND_MAX))
#elif _WIN32
#define CYCLIC_RANDOM(spec, mod) (rand() % (mod))
#define FLOAT_RANDOM(spec) (((float)rand()) / (float)RAND_MAX)
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

    COLLECTIVE_TOPOLOGY_ALL /* default, must be last */
} topology_type_t;

typedef enum model_type
{
    COLLECTIVE_MODEL_ITERATIVE = 0, /* Basic collective */
    COLLECTIVE_MODEL_PACKET_DELAY,  /* Random packet delay */
    COLLECTIVE_MODEL_PACKET_DROP,   /* Random failure at times */
    COLLECTIVE_MODEL_TIME_OFFSET,   /* Random start time offset */
	COLLECTIVE_MODEL_NODES_MISSING, /* Random nodes do not take part */
	COLLECTIVE_MODEL_NODES_FAILING, /* Radmon nodes fail online */
    COLLECTIVE_MODEL_ALL /* default, must be last */
} model_type_t;

typedef enum tree_recovery_type
{
    COLLECTIVE_RECOVERY_FATHER_FIRST = 0,
    COLLECTIVE_RECOVERY_BROTHER_FIRST,

    COLLECTIVE_RECOVERY_ALL /* default, must be last */
} tree_recovery_type_t;

typedef struct topology_spec
{
	int verbose;
	node_id my_rank;
	node_id node_count;
	unsigned char *my_bitfield;
	unsigned death_timeout;
	unsigned random_seed;
	unsigned step_index; /* struct abuse in favor of optimization */

	topology_type_t topology_type;
	union {
		struct {
		    unsigned radix;
		    tree_recovery_type_t recovery;
		} tree;
		struct {
		    unsigned radix;
		} butterfly;
	} topology;

	model_type_t model_type;
	union {
		float node_fail_rate;
		float packet_drop_rate;
		unsigned time_offset_max;
		unsigned packet_delay_max;
	} model;
} topology_spec_t;

enum topology_map_slot
{
	TREE = 0,
	BUTTERFLY,

	MAX /* must be last */
};

typedef struct topo_funcs
{
	size_t (*size_f)();
	int    (*build_f)(topology_spec_t *spec, comm_graph_t **graph);
	int    (*start_f)(topology_spec_t *spec, comm_graph_t *graph, void *internal_ctx);
	int    (*next_f)(comm_graph_t *graph, void *internal_ctx, node_id *target, unsigned *distance);
	int    (*fix_f)(comm_graph_t *graph, void *internal_ctx, tree_recovery_type_t method, node_id broken);
} topo_funcs_t;

typedef struct topology_iterator {
	comm_graph_t *graph;
	topology_spec_t *spec;
	topo_funcs_t funcs;
	unsigned time_offset;
	unsigned random_seed;
	unsigned time_finished;
	char ctx[0]; /* internal context for each iterator, type depends on topology */
} topology_iterator_t;

size_t topology_iterator_size();

int topology_iterator_create(topology_spec_t *spec, topology_iterator_t *iterator);

#define NO_PACKET ((unsigned)-1) /* set as distance */
#define IS_DEAD(iterator) (iterator->time_offset == -1)
#define SET_DEAD(iterator) ({ iterator->time_offset = -1; printf("\n\npronounced DEAD! \n\n"); })

int topology_iterator_next(topology_iterator_t *iterator, node_id *target, unsigned *distance);

int topology_iterator_omit(topology_iterator_t *iterator, tree_recovery_type_t method, node_id broken);

void topology_iterator_destroy(topology_iterator_t *iterator);

//int topology_test(collective_topology_t topology, node_id node_count);
