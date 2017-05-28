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
    COLLECTIVE_MODEL_BASE = 0,      /* Basic collective */
    COLLECTIVE_MODEL_SPREAD,        /* Random start time offset */
	COLLECTIVE_MODEL_NODES_MISSING, /* Random nodes do not take part */
	COLLECTIVE_MODEL_NODES_FAILING, /* Random nodes fail online */
    COLLECTIVE_MODEL_ALL /* default, must be last */
} model_type_t;

typedef enum tree_recovery_type
{
    COLLECTIVE_RECOVERY_FATHER_FIRST = 0,
    COLLECTIVE_RECOVERY_BROTHER_FIRST,
	COLLECTIVE_RECOVERY_CATCH_THE_BUS,

    COLLECTIVE_RECOVERY_ALL /* default, must be last */
} tree_recovery_type_t;

typedef struct topology_spec
{
	int verbose;
	node_id my_rank;
	node_id node_count;
	unsigned char *my_bitfield;
	unsigned random_seed;
	unsigned step_index; /* struct abuse in favor of optimization */
	unsigned latency;

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
		unsigned max_spread;
		float node_fail_rate;
	} model;
} topology_spec_t;

typedef struct send_item {
	node_id       dst;       /* packet destination */
#define           DESTINATION_UNKNOWN ((node_id)-1)
#define           DESTINATION_SPREAD  ((node_id)-2)
#define           DESTINATION_DEAD    ((node_id)-3)
	node_id       src;       /* packet source (sender) */
	msg_type      msg;       /* packet type (per-protocol) */
	unsigned      distance;  /* packet distance (time to be delayed in queue) */
#define           DISTANCE_VACANT (0)
#define           DISTANCE_NO_PACKET (0)
#define           DISTANCE_SEND_NOW (1)
	unsigned char *bitfield; /* pointer to the packet data */
#define           BITFIELD_FILL_AND_SEND (NULL)
} send_item_t;

typedef struct send_list {
	send_item_t *items;    /* List of stored items */
	void        *data;     /* Array of stored data (matches items array) */
	unsigned    allocated; /* Number of items allocated in memory */
	unsigned    used;      /* Number of items used (<= allocated) */
} send_list_t;

typedef struct topology_iterator {
	comm_graph_t *graph;        /* Pointer to the graph - changes upon node failures */
	send_list_t  in_queue;      /* Stores incoming messages to this node */
	unsigned     time_offset;   /* When did this node "join" the collective */
#define          TIME_OFFSET_DEAD ((unsigned)-1)
	unsigned     time_finished; /* When did this node "leave" the collective */
	char         ctx[0];        /* internal context, type depends on topology */
} topology_iterator_t;

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
	int    (*next_f)(comm_graph_t *graph, send_list_t *in_queue,
			         void *internal_ctx, send_item_t *result);
	int    (*fix_f)(comm_graph_t *graph, void *internal_ctx,
			        tree_recovery_type_t method, node_id broken);
} topo_funcs_t;

#define IS_DEAD(iterator) (iterator->time_offset == TIME_OFFSET_DEAD)
#define SET_DEAD(iterator) ({ iterator->time_offset = TIME_OFFSET_DEAD; })

size_t topology_iterator_size();

int topology_iterator_create(topology_spec_t *spec,
		                     topo_funcs_t *funcs,
							 topology_iterator_t *iterator);

int topology_iterator_next(topology_spec_t *spec, topo_funcs_t *funcs,
		                   topology_iterator_t *iterator, send_item_t *result);

int topology_iterator_omit(topology_iterator_t *iterator, topo_funcs_t *funcs,
		                   tree_recovery_type_t method, node_id broken);

void topology_iterator_destroy(topology_iterator_t *iterator);
