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
    COLLECTIVE_TOPOLOGY_OPTIMAL_TREE,
    COLLECTIVE_TOPOLOGY_DE_BROIJN,
    COLLECTIVE_TOPOLOGY_HYPERCUBE,
    COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING,

    COLLECTIVE_TOPOLOGY_ALL /* default, must be last */
} topology_type_t;

typedef enum model_type
{
    COLLECTIVE_MODEL_BASE = 0,      /* Basic collective */
    COLLECTIVE_MODEL_SPREAD,        /* Random start time offset */
    COLLECTIVE_MODEL_NODES_MISSING, /* Random nodes do not take part */
    COLLECTIVE_MODEL_NODES_FAILING, /* Random nodes fail online */
	COLLECTIVE_MODEL_REAL,          /* All other models combined */

    COLLECTIVE_MODEL_ALL            /* default, must be last */
} model_type_t;

typedef enum spread_distribution_type {
	SPREAD_DISTRIBUTION_UNIFORM = 0,
	SPREAD_DISTRIBUTION_NORMAL
} spread_distribution_type_t;

typedef enum collective_type {
	COLLECTIVE_TYPE_ALLREDUCE = 0,
	COLLECTIVE_TYPE_BROADCAST,
	COLLECTIVE_TYPE_REDUCE
} collective_type_t;

typedef enum tree_service_cycle_method {
	TREE_SERVICE_CYCLE_RANDOM = 0,
	TREE_SERVICE_CYCLE_CALC
} tree_service_cycle_method_t; // TODO: implement selection

typedef enum tree_recovery_method
{
    COLLECTIVE_RECOVERY_FATHER_FIRST = 0,
    COLLECTIVE_RECOVERY_BROTHER_FIRST,
    COLLECTIVE_RECOVERY_CATCH_THE_BUS,
    COLLECTIVE_RECOVERY_DISREGARD,

    COLLECTIVE_RECOVERY_ALL /* default, must be last */
} tree_recovery_method_t;

typedef struct topology_spec
{
    int verbose;
    node_id my_rank;
    node_id node_count;
    unsigned char *my_bitfield;
    size_t bitfield_size;
    unsigned random_seed;
    step_num step_index; /* struct abuse in favor of optimization */
    step_num latency;
    int async_mode;
    collective_type_t collective;
    long unsigned test_gen; /* protection from mixing packets between async. tests */

    topology_type_t topology_type;
    union {
        struct {
            unsigned radix;
            tree_recovery_method_t recovery;
            tree_service_cycle_method_t service;
        } tree;
        struct {
            unsigned radix;
        } butterfly;
        struct {
            unsigned degree;
        } hypercube;
    } topology;

    model_type_t model_type;
    struct {
    	tree_service_cycle_method_t service_mode;
    	spread_distribution_type_t spread_mode;
    	step_num spread_avg;

        float offline_fail_rate; /* if >1 - absolute, if <1 - percentage of nodes */
        float online_fail_rate; /* if >1 - absolute, if <1 - rate out of total nodes */
    } model;
} topology_spec_t;

typedef struct send_item {
    node_id       dst;       /* packet destination */
#define           DESTINATION_UNKNOWN  ((node_id)-1)
#define           DESTINATION_SPREAD   ((node_id)-2)
#define           DESTINATION_DEAD     ((node_id)-3)
#define           DESTINATION_IDLE     ((node_id)-4)
    node_id       src;       /* packet source (sender) */
    msg_type      msg;       /* packet type (per-protocol) */
#define           MSG_DEATH            ((msg_type)-1)
    step_num      distance;  /* packet distance (time to be delayed in queue) */
#define           DISTANCE_VACANT      (0)
#define           DISTANCE_NO_PACKET   (0)
#define           DISTANCE_SEND_NOW    (1)
    step_num      timeout;   /* packet timeout (after which consider peer dead),
                                after subtracting the initial send distance */
#define TIMEOUT_NEVER ((step_num)-1) /* Packet does not expect a response (protocol-specific) */
    long unsigned test_gen;  /* protection from mixing packets between async. tests */
    unsigned char *bitfield; /* pointer to the packet data - MUST BE LAST MEMBER */
#define           BITFIELD_IGNORE_DATA (NULL)
} send_item_t;

typedef struct send_list {
    send_item_t   *items;    /* List of stored items */
    unsigned char *data;     /* Array of stored data (matches items array) */
    unsigned      allocated; /* Number of items allocated in memory */
    unsigned      used;      /* Number of items used (<= allocated) */
    unsigned      next;      /* Next location to start looking for vacancy from */
    unsigned      max;       /* Maximal number of pending elements ever */
} send_list_t;

/* To be called from wthin individual topologies */
int global_enqueue(send_item_t *sent, send_list_t *queue, unsigned bitfield_size);

typedef struct topology_iterator {
    comm_graph_t *graph;        /* Pointer to the graph - changes upon node failures */
    send_list_t  in_queue;      /* Stores incoming messages to this node */

    step_num     start_offset;   /* When does this node start the collective algorithm */
    step_num     death_offset;   /* When does this node stop the collective algorithm */
#define          NODE_IS_DEAD    ((step_num)-1)
#define          NODE_IS_IMORTAL ((step_num)-2)

    step_num     finish;        /* When did this node "leave" the collective */
    char         ctx[0];        /* internal context, type depends on topology */
} topology_iterator_t;

enum topology_map_slot {
    TREE = 0,
    OPTIMAL,
    DE_BRUIJN,
    HYPERCUBE,
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
                    tree_recovery_method_t method,
					node_id source, int source_is_dead);
    void   (*stop_f)(void *internal_ctx);
} topo_funcs_t;

#define IS_DEAD(iterator) (iterator->death_offset == NODE_IS_DEAD)
#define SET_DEAD(iterator) ({ iterator->death_offset = NODE_IS_DEAD; })

size_t topology_iterator_size();

int topology_iterator_create(topology_spec_t *spec,
                             topo_funcs_t *funcs,
                             topology_iterator_t *iterator);

int topology_iterator_next(topology_spec_t *spec,
                           topo_funcs_t *funcs,
                           topology_iterator_t *iterator,
						   send_list_t *global_queue,
						   send_item_t *result);

int topology_iterator_omit(topology_iterator_t *iterator,
						   topo_funcs_t *funcs,
                           tree_recovery_method_t method,
						   node_id source,
						   int source_is_dead);

void topology_iterator_destroy(topology_iterator_t *iterator, topo_funcs_t *funcs);
