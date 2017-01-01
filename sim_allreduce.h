/*
 ============================================================================
 Name        : sim_coll.c
 Author      : Alex Margolin
 Version     : 1.0
 Description : Test Allreduce collective
 ============================================================================
 */

//#define MPI_SPLIT_TESTS
//#define TEST_FIRST

#if defined(MPI_SPLIT_PROCS) || defined(MPI_SPLIT_TESTS)
#include <mpi.h>
#endif

#define PERROR printf
#define VERBOSE_PROC_THRESHOLD (0)
#define MULTIPROCESS_ABSOLUTE_THRESHOLD (128)
#define MULTIPROCESS_RELATIVE_THRESHOLD (1)

typedef enum collective_model
{
    COLLECTIVE_MODEL_ITERATIVE = 0, /* Basic collective */
    COLLECTIVE_MODEL_PACKET_DELAY,   /* Random packet delay */
    COLLECTIVE_MODEL_PACKET_DROP,   /* Random failure at times */
    COLLECTIVE_MODEL_TIME_OFFSET,   /* Random start time offset */

    COLLECTIVE_MODEL_ALL /* default, must be last */
} collective_model_t;

typedef enum collective_topolgy
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
} collective_topology_t;

typedef struct topology_iterator topology_iterator_t;
