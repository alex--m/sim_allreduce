#include <stdlib.h>
#include <string.h>

#define OK (0)
#define ERROR (-1)

typedef unsigned long node_id;
typedef unsigned group_id; /* each group simulates multiple nodes */

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

// Utility function to protect against integer overflow
#if defined( _MSC_VER )
__forceinline
#endif
size_t get_intel_flags(const size_t bitmask)
{
#if defined( _MSC_VER )
    return __readeflags() & bitmask;
#elif defined( __GNUC__ )
    size_t flags;
    __asm__ __volatile__(
            "pushfq\n"
            "pop %%rax\n"
            "movq %%rax, %0\n"
            :"=r"(flags)
             :
             :"%rax"
    );
    return flags & bitmask;
#else
#define OVERFOW_UNSUPPORTED
#pragma message("Inline assembly not supported.")
    return OK;
#endif
}

#ifdef TEST_FIRST
#define HAS_OVERFLOWN (get_intel_flags(0x801))
    // 0x800 is signed overflow and 0x1 is carry
#define BREAK_ON_OVERFLOW if (HAS_OVERFLOWN) break;
#define RETURN_ON_OVERFLOW(x) if (HAS_OVERFLOWN) { printf("ERROR!!\n\n"); return (x);}
#else
#define HAS_OVERFLOWN (0)
#define BREAK_ON_OVERFLOW
#define RETURN_ON_OVERFLOW(x)
#endif
