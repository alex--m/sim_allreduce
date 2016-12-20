/*
 ============================================================================
 Name        : sim_coll.c
 Author      : Alex Margolin
 Version     : 1.0
 Description : Test Allreduce collective
 ============================================================================
 */

#define MPI_SPLIT_TESTS
//#define TEST_FIRST

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
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

struct stats {
    unsigned long cnt;
    unsigned long sum;
    unsigned long min;
    unsigned long max;
    float avg;
};

struct collective_iteration_ctx;
typedef struct collective_spec
{
    collective_model_t model;
    collective_topology_t topology;

    unsigned test_count;
    unsigned step_index;
    unsigned step_count; /* 0 to run until -1 is returned */
    unsigned tree_radix;
    unsigned cycle_const;
    unsigned cycle_random;
    unsigned random_seed;
    unsigned random_up;
    unsigned random_down;
    unsigned delay_max;
    unsigned offset_max;
    unsigned failover;
    float fail_rate;

    unsigned proc_group_index;
    unsigned proc_group_count;
    unsigned proc_group_size;
    unsigned proc_total_size;
    unsigned bitfield_size;

#ifdef MPI_SPLIT_TESTS
    unsigned test_node_index;
    unsigned test_node_count;
#endif

    unsigned long messages_counter;
    unsigned long data_len_counter;

    struct stats steps;
    struct stats data;
    struct stats msgs;

    /* Optimization of buffer reuse, when possible */
    unsigned last_proc_total_size;
    struct collective_iteration_ctx *ctx;
} collective_spec_t;

typedef struct collective_plan
{
    unsigned peer_next;
    unsigned peer_count;
    struct {
        unsigned peer_rank;
        unsigned is_send;
    } peer[0];
} collective_plan_t;

#ifdef MPI_SPLIT_PROCS
typedef struct exchange_plan
{
    unsigned packet_count;
    unsigned is_full;
} exchange_plan_t;
#endif

typedef struct collective_datagram
{
    unsigned source_rank;
    unsigned dest_rank;
    unsigned delay;
    union {
        char bitfield[0];
        void *sent;
    };
} collective_datagram_t;

typedef struct collective_iteration_ctx
{
    unsigned my_rank;          /* Global rank (0..N) */
    unsigned my_local_rank;    /* Local rank (< group_size) */

    unsigned char *new_matrix; /* Matrix of bitwise information by source ranks */
    unsigned char *old_matrix; /* Previous step of the matrix */

    unsigned *time_offset;     /* Time from job start before starting collective */
    collective_plan_t **plans; /* Plan of operations (where to send/recv next) */
    unsigned planned;          /* Amount of packets stored */

    collective_datagram_t *storage; /* Storing packets sent on each iteration */
    unsigned stored;           /* Amount of packets stored */

    collective_spec_t *spec;   /* Properties of a single collective test */

#ifdef MPI_SPLIT_PROCS
    exchange_plan_t targets;     /* Coordination info to send to each peer */
    collective_datagram_t *packets; /* Storing packets sent on each iteration */
    unsigned packet_count;          /* Total packet count in the storage */
#endif
} collective_iteration_ctx_t;




/*****************************************************************************\
 *                                                                           *
 *                              Bitfield Macros                              *
 *                                                                           *
\*****************************************************************************/

// Optimization: first byte is marked once full to avoid repeated checks

#define CALC_BITFIELD_SIZE(size) (1 + ((size) >> 3) + ((size & 7) != 0))

#define CTX_BITFIELD_SIZE(ctx) ((ctx)->spec->bitfield_size)

#define CTX_MATRIX_SIZE(ctx) \
    ((ctx)->spec->proc_group_size * CTX_BITFIELD_SIZE(ctx))

#define GET_OLD_BITFIELD(ctx, local_proc) ((ctx)->old_matrix + \
    (((local_proc) % ((ctx)->spec->proc_group_size)) * CTX_BITFIELD_SIZE(ctx)))

#define GET_NEW_BITFIELD(ctx, local_proc) ((ctx)->new_matrix + \
    (((local_proc) % ((ctx)->spec->proc_group_size)) * CTX_BITFIELD_SIZE(ctx)))

#define SET_BIT(ctx, local_proc, proc_bit) \
    *(GET_NEW_BITFIELD(ctx, local_proc) + 1 + ((proc_bit) >> 3)) |= \
        (1 << ((proc_bit) & 7))

#define IS_BIT_SET(ctx, local_proc, proc_bit) \
    ((*(GET_OLD_BITFIELD(ctx, local_proc) + 1 + ((proc_bit) >> 3)) & \
        (1 << ((proc_bit) & 7))) != 0)

#define IS_MY_BIT_SET(ctx, proc_bit) IS_BIT_SET(ctx, ctx->my_rank, proc_bit)

#define SET_UNUSED_BITS(ctx) ({                                               \
    unsigned proc, bit, bit_max = ((CTX_BITFIELD_SIZE(ctx) - 1) << 3);        \
    for (proc = 0; proc < (ctx)->spec->proc_group_size; proc++) {             \
        for (bit = (ctx)->spec->proc_total_size; bit < bit_max; bit++) {      \
            SET_BIT((ctx), proc, bit);                                        \
        }                                                                     \
    }                                                                         \
})

#define IS_BUFFER_FULL(buffer, len) ({                                        \
    int i = 0, is_full = 1;                                                   \
    if (buffer[0] == 0) {                                                     \
        buffer[0] = (char)-1;                                                 \
        while (is_full && ((sizeof(int) * (i + 1)) < (len))) {                \
            is_full = is_full && ((int)-1 == *((int*)(buffer) + i++));        \
        }                                                                     \
        i *= sizeof(int);                                                     \
        while (is_full && (i < (len))) {                                      \
            is_full = is_full && ((char)-1 == *((char*)(buffer) + i++));      \
        }                                                                     \
        buffer[0] = is_full;                                                  \
    }                                                                         \
    is_full;                                                                  \
})

#define IS_FULL(ctx, local_proc) \
    IS_BUFFER_FULL(GET_NEW_BITFIELD((ctx), local_proc), CTX_BITFIELD_SIZE(ctx))

#define IS_MINE_FULL(ctx) IS_FULL((ctx), ctx->my_rank)

#define IS_ALL_FULL(ctx) ({                                                   \
    int j = 0, is_full = 1;                                                   \
    while (is_full && (j < (ctx)->spec->proc_group_size)) {                   \
        is_full = IS_BUFFER_FULL(GET_NEW_BITFIELD(ctx, j),                    \
                CTX_BITFIELD_SIZE(ctx));                                      \
        j++;                                                                  \
    }                                                                         \
    is_full;                                                                  \
})

#define POPCOUNT(ctx) ({                                                      \
    unsigned i, count = 0;                                                    \
    for (i = 0; i < (ctx)->spec->proc_total_size; i++) {                      \
        count += IS_MY_BIT_SET((ctx), i);                                     \
    }                                                                         \
    count;                                                                    \
})

#define MERGE(ctx, local_proc, addition) ({                                   \
    unsigned i = 0, popcnt = 0, max = CTX_BITFIELD_SIZE(ctx);                 \
    unsigned char *present = GET_NEW_BITFIELD(ctx, local_proc);               \
    ctx->spec->messages_counter++;                                            \
    while (sizeof(unsigned) * (i + 1) < max)                                  \
    {                                                                         \
        unsigned added = *((unsigned*)(addition) + i);                        \
        *((unsigned*)present + i) |= added;                                   \
        popcnt += __builtin_popcount(added);                                  \
        i++;                                                                  \
    }                                                                         \
    ctx->spec->data_len_counter += popcnt;                                    \
    popcnt = 0;                                                               \
    i *= sizeof(unsigned);                                                    \
    while (i < max)                                                           \
    {                                                                         \
        unsigned char added = (addition)[i];                                  \
        popcnt = (popcnt << 8) | added;                                       \
        present[i] |= added;                                                  \
        i++;                                                                  \
    }                                                                         \
    ctx->spec->data_len_counter +=                                            \
        __builtin_popcount(popcnt) - (((addition)[0]) == 1);                  \
})

#define MERGE_LOCAL(ctx, local_proc, added_proc) \
    MERGE(ctx, local_proc, GET_OLD_BITFIELD(ctx, added_proc))

#define PRINT(ctx) ({                                                         \
    int i;                                                                    \
    for (i = 0; i < (ctx)->spec->proc_total_size; i++) {                      \
        printf("%i", IS_MY_BIT_SET((ctx), i));                                \
    }                                                                         \
})

/*****************************************************************************\
 *                                                                           *
 *                              Tree Definitions                             *
 *                                                                           *
\*****************************************************************************/

typedef int(*exchange_f)(unsigned dest_rank, int is_send,
                         collective_iteration_ctx_t* ctx);

typedef int(*send_tree_f)(
        unsigned host_list_size,
        unsigned my_index_in_list,
        unsigned tree_radix,
        int is_multiroot,
        exchange_f target_rank_cb,
        collective_iteration_ctx_t *ctx);

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

/*****************************************************************************\
 *                                                                           *
 *                              Tree implementations                         *
 *                                                                           *
\*****************************************************************************/

/*****************************************************************************\
 *                                                                           *
 *                              Tree Test section                            *
 *                                                                           *
\*****************************************************************************/

#ifdef TEST_FIRST
typedef struct test_rank {
    int has_sent;
    unsigned parent;
    unsigned level;
    unsigned child_count;
} test_rank_t;

typedef struct test_ctx {
    collective_iteration_ctx_t padding;

    char *test_name;
    unsigned rank_count;
    unsigned rank_parent;
    unsigned max_root;
    test_rank_t *ranks;
    unsigned **children;
} test_ctx_t;

int test_pass(unsigned rank, int is_send, test_ctx_t *ctx)
{
    return OK;
}

int test_exchange_tree_cb(unsigned dest_rank, int is_send, test_ctx_t *ctx)
{
    unsigned index, my_rank = ctx->rank_parent;

    if (is_send) {
        if (my_rank == dest_rank) {
            PERROR("%i sending to himself!\n", my_rank);
            return ERROR;
        }

        if ((strcmp(ctx->test_name, "send_recursive_k_ing")) &&
            (!ctx->ranks[my_rank].has_sent) &&
            (dest_rank < my_rank) &&
            (ctx->ranks[my_rank].parent != dest_rank)) {
            PERROR("%i already has two parents: %i, %i !\n",
                   my_rank, ctx->ranks[my_rank].parent, dest_rank);
            return ERROR;
        }

        ctx->ranks[my_rank].has_sent = 1;
        return OK;
    }

    if (my_rank == dest_rank) {
        PERROR("%i receiving from himself!\n", my_rank);
        return ERROR;
    }

    // sanity check - make sure we use ranks within test size limits
    if (dest_rank >= ctx->rank_count)
    {
        PERROR("%s: child rank %u exceeds test size %u !\n",
                ctx->test_name, dest_rank, ctx->rank_count);
        return ERROR;
    }

    // skip check on iterations after the node information has been sent
    if (ctx->ranks[my_rank].has_sent) {
        return OK;
    }

    // make sure child array has no duplicates
    for (index = 0; index < ctx->ranks[my_rank].child_count; index++) {
        if (ctx->children[my_rank][index] == dest_rank) {
            PERROR("rank %i has duplicate child %i\n", my_rank, dest_rank);
            return ERROR;
        }
    }

    // set the necessary fields
    ctx->ranks[dest_rank].parent = my_rank;
    ctx->ranks[my_rank].level  = ctx->ranks[dest_rank].level + 1;
    ctx->children[my_rank][ctx->ranks[my_rank].child_count++] = dest_rank;

    // sanity check - make sure we don't exceed the children array limit
    if (ctx->ranks[my_rank].child_count == ctx->rank_count) {
        PERROR("%s: too many children for %i :",
               ctx->test_name, my_rank);
        for (index = 0; index < ctx->rank_count; index++) {
            printf("%i, ", ctx->children[my_rank][index]);
        }
        printf("\n");
        return ERROR;
    }

    return OK;
}

int test_tree(unsigned test_size, unsigned test_radix,
        send_tree_f tree_f, char* test_name)
{
    unsigned index, jndex, found, is_multiroot;
    collective_spec_t spec = {0};
    test_ctx_t ctx = {{0},0};
    int res = OK;

    ctx.padding.spec = &spec;
    ctx.test_name = test_name;
    ctx.rank_count = test_size;

    printf("\n Now testing send pass...\n");
    for (index = 0; (res == 0) && (index < test_size); index++) {
        res = tree_f(test_size, 0, test_radix, 0, (exchange_f)test_pass,
                     (collective_iteration_ctx_t*)&ctx);
    }

    if (res == ERROR) {
        PERROR("send pass failed!\n");
        return ERROR;
    }

    if (test_size == (unsigned)-1) {
        PERROR("Test size exceeded!\n");
        return ERROR;
    }

    ctx.ranks = calloc(test_size, sizeof(test_rank_t));
    if (!ctx.ranks) {
        PERROR("Allocation failed!\n");
        return ERROR;
    }

    ctx.children = malloc(test_size * sizeof(unsigned*));
    if (!ctx.children) {
        PERROR("Allocation failed!\n");
        return ERROR;
    }

    for (index = 0; index < test_size; index++) {
        ctx.children[index] = malloc(test_size * sizeof(unsigned));
        if (!ctx.children[index]) {
            PERROR("Allocation failed!\n");
            return ERROR;
        }
    }

    printf("Testing %s with size=%u radix=%u\n",
            test_name, test_size, test_radix);

    for (is_multiroot = 0; is_multiroot < 2; is_multiroot++) {
        /*
         * Run a test-collective on the entire tree
         */
        memset(ctx.ranks, 0, test_size * sizeof(test_rank_t));

        /* For each host in the test */
        for (index = 0; index < test_size; index++) {
            unsigned count_ranks_before = 0;
            unsigned count_ranks_after = 0;
            unsigned kndex;

            ctx.rank_parent = index;
            ctx.max_root = is_multiroot * test_radix;

            for (kndex = 0; kndex < test_size; kndex++) {
                count_ranks_before += (ctx.ranks[kndex].level != 0);
            }

            res = tree_f(test_size, index, test_radix, is_multiroot,
                        (exchange_f)test_exchange_tree_cb,
                        (collective_iteration_ctx_t*)&ctx);
            if (res == ERROR) {
                PERROR("tree function returned an error!\n");
                return ERROR;
            }

            for (kndex = 0; kndex < test_size; kndex++) {
                count_ranks_after += (ctx.ranks[kndex].level != 0);
            }

            if (count_ranks_before + 1 < count_ranks_after) {
                PERROR("more that one send on iteration #%i (%i->%i)!\n",
                        jndex, count_ranks_before, count_ranks_after);
                return ERROR;
            }
        }

        /*
         * Test the correctness of the tree (for every node)
         */
        for (index = ctx.max_root + 1; index < test_size; index++) {
            // make sure the node sent its information somewhere
            if (!ctx.ranks[index].has_sent) {
                printf("\nERROR with %s: rank %u hasn't sent his value!\n",
                                       test_name, index);
                return ERROR;
            }

            // make sure all children have this node listed as its parent
            for (jndex = 0; jndex < ctx.ranks[index].child_count; jndex++) {
                int child = ctx.children[index][jndex];
                if (ctx.ranks[child].parent != index) {
                    PERROR("%s: rank %u is bad father to %u!\n",
                           test_name, index, child);
                    return ERROR;
                }
            }

            // make sure the node parent has it listed as its child
            for (found = 0, jndex = 0;
                 (!found) && (jndex < test_size);
                 jndex++) {
                found = (ctx.children[ctx.ranks[index].parent][jndex] == index);
            }
            if ((strcmp(ctx.test_name, "send_recursive_k_ing")) && !found) {
                PERROR("%s: rank %u doesn't have child %u!\n",
                        test_name, ctx.ranks[index].parent, index);
                return ERROR;
            }
        }
    }

    for (index = 0; index < test_size; index++) {
        free(ctx.children[index]);
    }
    free(ctx.children);
    free(ctx.ranks);
    return OK;
}

int test_tree_implementation()
{
    char *names[] = {
            "send_narray_tree",
            "send_knomial_tree",
            "send_recursive_k_ing",
    };
    send_tree_f trees[] = {
            send_narray_tree,
            send_knomial_tree,
            send_recursive_k_ing,
    };
    unsigned test_size, radix, tree;
    for (test_size = 1; test_size < 1001; test_size *= 10)
    {
        // test some normal radixes
        for (radix = 2; radix < 11; radix++)
        {
            for (tree = 0; tree < (sizeof(trees)/sizeof(*trees)); tree++)
            {
                if (test_tree(test_size, radix, trees[tree], names[tree]) ==
                        ERROR)
                {
                    return ERROR;
                }
            }
        }
    }
    return OK;
}
#endif

/*****************************************************************************\
 *                                                                           *
 *                              Collective Iterations                        *
 *                                                                           *
\*****************************************************************************/

#ifdef __linux__
#define CYCLIC_RANDOM(spec, mod) (rand_r(&(spec)->random_seed) % (mod))
#define FLOAT_RANDOM(spec) ((rand_r(&(spec)->random_seed)) / ((float)RAND_MAX))
#elif _WIN32
#define CYCLIC_RANDOM(spec, mod) (rand() % (mod))
#define FLOAT_RANDOM(spec) (((float)rand()) / RAND_MAX)
#else
#error "OS not supported!"
#endif

static int sim_coll_send(unsigned dest_rank, unsigned distance,
                         collective_iteration_ctx_t* ctx, char *sent_bitfield)
{
#ifdef MPI_SPLIT_PROCS
    if (dest_rank / ctx->spec->proc_group_size != ctx->spec->proc_group_index) {
        collective_datagram_t *pkt = &ctx->packets[ctx->packet_count++];
        ctx->targets[dest_rank / ctx->spec->proc_group_size].packet_count++;
        pkt->source_rank = ctx->my_rank;
        pkt->dest_rank = dest_rank;
        pkt->sent = sent_bitfield;
        return OK;
    }
#endif

    if (distance) {
        void *new_storage;
        unsigned storage_iter = 0;
        collective_datagram_t *slot = ctx->storage;
        unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);

        /* Look for an available slot to store the message */
        while ((storage_iter < ctx->stored) && (slot->delay != 0)) {
            slot = (collective_datagram_t*)((char*)slot + slot_size);
            storage_iter++;
        }
        
        if (storage_iter == ctx->stored) {
            collective_datagram_t *extra_iter;

            /* Allocate extra storage space */
            ctx->stored <<= 1;
            new_storage = realloc(ctx->storage, ctx->stored * slot_size);
            if (!new_storage) {
                PERROR("Allocation Failed!\n");
                return ERROR;
            }
            
            /* Move to the new array of slots */
            slot = (collective_datagram_t*)(((char*)slot -
                    (char*)ctx->storage) + (char*)new_storage);
            ctx->storage = new_storage;
            extra_iter = slot;
            
            /* Mark the newest slots as free */
            while (storage_iter < ctx->stored) {
                extra_iter->delay = 0; /* Initialize the new slots as empty */
                extra_iter = (collective_datagram_t*)((char*)extra_iter + slot_size);
                storage_iter++;
            }
        }

        slot->source_rank = ctx->my_rank;
        slot->dest_rank = dest_rank;
        slot->delay = distance + 2;
        memcpy(&slot->bitfield, GET_OLD_BITFIELD(ctx, ctx->my_rank),
               CTX_BITFIELD_SIZE(ctx));
        return OK;
    }

    if (sent_bitfield) {
        MERGE(ctx, dest_rank, sent_bitfield);
    } else {
        MERGE_LOCAL(ctx, dest_rank, ctx->my_rank);
    }
    return OK;
}

static int sim_coll_process(collective_iteration_ctx_t *ctx)
{
    int res = OK;
    unsigned iter;
    unsigned next_target;
    unsigned expected_rank;
    unsigned distance = 0;
    unsigned cycle_len = 0;
    collective_plan_t *plan;

    if ((ctx->spec->model == COLLECTIVE_MODEL_PACKET_DROP) &&
        (ctx->spec->fail_rate > FLOAT_RANDOM(ctx->spec))) {
        ctx->spec->random_up++;
        return OK;
    }
    ctx->spec->random_down++;

    if ((ctx->spec->model == COLLECTIVE_MODEL_TIME_OFFSET) &&
        (ctx->spec->step_index < ctx->time_offset[ctx->my_local_rank])) {
        return OK;
    }

    if (ctx->spec->model == COLLECTIVE_MODEL_PACKET_DELAY) {
        distance = CYCLIC_RANDOM(ctx->spec, ctx->spec->delay_max);
    }


    switch (ctx->spec->topology)
    {
    case COLLECTIVE_TOPOLOGY_RANDOM_PURE:
        /* Send to a random node */
        next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 2> random steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix - 1> const steps */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
    case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
        cycle_len = ctx->spec->cycle_random + ctx->spec->cycle_const;
        if ((ctx->spec->step_index % cycle_len) < ctx->spec->cycle_random) {
            next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        } else {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + ctx->spec->step_index) %
                    ctx->spec->proc_total_size;
        }

        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    case COLLECTIVE_TOPOLOGY_RANDOM_HEURISTIC: /* Send to missing nodes from bit-field, the 1 radom for <radix> const steps */
        if ((IS_MINE_FULL(ctx)) && (ctx->spec->step_index % ctx->spec->tree_radix))
        {
            next_target = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size);
        } else if (IS_MINE_FULL(ctx)) {
            /* Send to a node of increasing distance */
            next_target = (ctx->my_rank + ctx->spec->step_index) %
                    ctx->spec->proc_total_size;
        } else {
            /* Send to a random node missing from my (incoming) bitfield */
            iter = CYCLIC_RANDOM(ctx->spec, ctx->spec->proc_total_size - POPCOUNT(ctx)) + 1;
            next_target = 0;
            while (iter) {
                if (!IS_MY_BIT_SET(ctx, next_target)) {
                    iter--;
                }
                next_target++;
            }
            next_target--;
        }

        res = sim_coll_send(next_target, distance, ctx, NULL);
        break;

    default:
        plan = ctx->plans[ctx->my_local_rank];

        /* Wait for incoming messages if planned */
        while ((plan->peer_next < plan->peer_count) &&
               (!plan->peer[plan->peer_next].is_send)) {
            expected_rank = plan->peer[plan->peer_next].peer_rank;
            if (IS_MY_BIT_SET(ctx, expected_rank)) {
                plan->peer_next++;
            } else {
                break;
            }
        }

        /* Send one outgoing message if planned */
        if ((plan->peer_next < plan->peer_count) &&
            (plan->peer[plan->peer_next].is_send)) {
            expected_rank = plan->peer[plan->peer_next].peer_rank;
            res = sim_coll_send(expected_rank, distance, ctx, NULL);
            plan->peer_next++;
        }
    }

    if (res != OK) {
        return res;
    }

    /* Update cycle proportions for random hybrids */
    if (cycle_len && (ctx->spec->step_index % cycle_len == 0)) {
        switch (ctx->spec->topology)
        {
        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
            ctx->spec->cycle_const++;
            break;

        case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
            ctx->spec->cycle_const <<= 1;
            break;

        default:
            break;
        }
    }

    /* Treat pending messages if storage is relevant (distance between procs) */
    if (ctx->stored) {
        collective_datagram_t *slot;
        unsigned my_rank = ctx->my_rank;
        unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);

        /* Decrement send candidates */
        for (iter = 0, slot = ctx->storage;
             iter < ctx->stored;
             iter++,
             slot = (collective_datagram_t*)((char*)slot + slot_size)) {
            if ((slot->delay != 0) && (slot->source_rank == my_rank)) {
                slot->delay--;
                if (slot->delay == 1) {
                    res = sim_coll_send(slot->dest_rank, 0, ctx, &slot->bitfield[0]);
                    if (res != OK) {
                        return res;
                    }
                }
            }
        }
    }

    /* Optionally, output debug information */
    if ((ctx->spec->test_count == 1) &&
        (ctx->spec->proc_total_size < VERBOSE_PROC_THRESHOLD))
    {
        printf("\nstep=%2i proc=%3i popcount:%3i/%3i ", ctx->spec->step_index,
               ctx->my_rank, POPCOUNT(ctx), ctx->spec->proc_total_size);
        PRINT(ctx);
    }

    return res;
}

#ifdef MPI_SPLIT_PROCS
static int sim_coll_mpi_exchange(collective_iteration_ctx_t *ctx) {
    MPI_Status *statuses;
    MPI_Request *requests;
    collective_datagram_t *incoming, *outgoing, *next;
    size_t dgram_size = CTX_BITFIELD_SIZE(ctx) + sizeof(collective_datagram_t);
    int index, ret_val, incoming_cnt, outgoing_cnt, req_cnt = 0;

    exchange_plan_t *recv_iter, *recv_plan =
            alloca(ctx->spec->proc_group_count * sizeof(*recv_plan));

    // Count how many full ranks are on the in the local group
    ctx->targets[0].is_full = IS_ALL_FULL(ctx);
    for (index = 0; index < ctx->spec->proc_group_count; index++) {
        ctx->targets[index].is_full = ctx->targets[0].is_full;
    }

    // Collect how many sends from each proc, and how much is left to do
    ret_val = MPI_Alltoall(ctx->targets, 1, MPI_2INT, recv_plan, 1, MPI_2INT,
                           MPI_COMM_WORLD);
    if (ret_val != MPI_SUCCESS) {
        return ERROR;
    }

    // Prepare for incoming messages and check if finished
    for (ret_val = 1, incoming_cnt = 0, outgoing_cnt=0, index = 0;
         index < ctx->spec->proc_group_count;
         index++) {
        incoming_cnt += recv_plan[index].packet_count;
        outgoing_cnt += ctx->targets[index].packet_count;
        ret_val = ret_val && recv_plan[index].is_full;
    }

    if (ret_val) {
        return OK; // All full - all done!
    }

    // Allocate requests
    req_cnt = incoming_cnt + outgoing_cnt;
    requests = malloc(req_cnt * sizeof(MPI_Request));
    if (requests == NULL) {
        ret_val = ERROR;
        goto ret_err;
    }

    statuses = malloc(req_cnt * sizeof(MPI_Status));
    if (statuses == NULL) {
        ret_val = ERROR;
        goto free_req;
    }

    incoming = malloc(incoming_cnt * dgram_size);
    if (incoming == NULL) {
        ret_val = ERROR;
        goto free_stat;
    }

    outgoing = malloc(outgoing_cnt * dgram_size);
    if (outgoing == NULL) {
        ret_val = ERROR;
        goto free_in;
    }

    // Launch requests for incoming datagrams
    for (recv_iter = recv_plan, next = incoming, index = 0;
         index < incoming_cnt;
         next = (void*)(((char*)next) + dgram_size),
         index++, recv_iter->packet_count--) {
        // Find next non-empty batch of incoming packets to expect
        while (recv_iter->packet_count == 0) {
            recv_iter++;
        }

        // Launch incoming packet request
        ret_val = MPI_Irecv(next, dgram_size, MPI_CHAR, recv_iter - recv_plan,
                            0, MPI_COMM_WORLD, requests + index);
        if (ret_val != MPI_SUCCESS) {
            goto free_out;
        }
    }

    // Launch requests for outgoing datagrams
    for (next = outgoing, index = 0; index < outgoing_cnt;
         next = (void*)(((char*)next) + dgram_size), index++) {
        // Fill in packet contents
        memcpy(next, ctx->packets + index, sizeof(collective_datagram_t));
        memcpy(next + 1, GET_OLD_BITFIELD(ctx, next->source_rank),
               CTX_BITFIELD_SIZE(ctx));

        // Send packet to destination
        ret_val = MPI_Isend(next, dgram_size, MPI_CHAR,
                            next->dest_rank / ctx->spec->proc_group_size,
                            0, MPI_COMM_WORLD, requests + incoming_cnt + index);
        if (ret_val != MPI_SUCCESS) {
            goto free_out;
        }
    }

    // Wait for all requests to complete
    ret_val = MPI_Waitall(req_cnt, requests, statuses);
    if (ret_val != MPI_SUCCESS) {
        ret_val = ERROR;
        goto free_out;
    }

    for (index = 0; index < req_cnt; index++) {
        if (ret_val != MPI_SUCCESS) {
            ret_val = ERROR;
            goto free_out;
        }
    }

    // Apply incoming information to the matrix
    for (next = incoming, index = 0;
         index < incoming_cnt;
         next = (void*)(((char*)next) + dgram_size), index++) {
        MERGE(ctx, next->dest_rank, next->bitfield);
    }

    ctx->packet_count = 0;
    memset(ctx->targets, 0,
           ctx->spec->proc_group_count * sizeof(*ctx->targets));
    ret_val = BLOCKED_WAITING; // Have more work to do...

free_out:
    free(outgoing);
free_in:
    free(incoming);
free_stat:
    free(statuses);
free_req:
    free(requests);
ret_err:
    return ret_val;
}
#endif

static int sim_coll_target(unsigned dest_rank, int is_send,
                           collective_iteration_ctx_t* ctx)
{
    collective_plan_t *plan = ctx->plans[ctx->my_local_rank];
    if (plan->peer_next == plan->peer_count) {
        plan->peer_count <<= 1;
        plan = realloc(plan, sizeof(collective_plan_t) +
                plan->peer_count * 2 * sizeof(unsigned));
        if (!plan) {
            PERROR("Allocation failed!\n");
            return ERROR;
        }
        ctx->plans[ctx->my_local_rank] = plan;
    }

    plan->peer[plan->peer_next].peer_rank = dest_rank;
    plan->peer[plan->peer_next].is_send = is_send;
    plan->peer_next++;
    return OK;
}

int sim_coll_plan(collective_iteration_ctx_t *ctx)
{
    collective_plan_t *plan;
    unsigned rank, local_rank;
    int res = OK, is_tree_multiroot = 0;
    unsigned rank_count = ctx->spec->proc_group_size;
    unsigned first_rank = ctx->spec->proc_group_index *
            ctx->spec->proc_group_size;

    ctx->planned = rank_count;
    ctx->plans = malloc(rank_count * sizeof(void*));
    if (!ctx->plans) {
        PERROR("Allocation failed!\n");
        return ERROR;
    }

    for (rank = first_rank; rank < first_rank + rank_count; rank++) {
        local_rank = ctx->my_local_rank = rank - first_rank;
        ctx->my_rank = rank;

        plan = malloc(sizeof(collective_plan_t) + 2 * sizeof(unsigned));
        if (!plan) {
            while (rank > first_rank) {
                free(ctx->plans[(rank--) - 1]);
            }

            PERROR("Allocation failed!\n");
            return ERROR;
        }

        ctx->plans[local_rank] = plan;
        plan->peer_count = 1;
        plan->peer_next = 0;

        switch (ctx->spec->topology) {
        case COLLECTIVE_TOPOLOGY_NARRAY_MULTIROOT_TREE:
            is_tree_multiroot = 1;
        case COLLECTIVE_TOPOLOGY_NARRAY_TREE:
            res = send_narray_tree(ctx->spec->proc_total_size, rank,
                                   ctx->spec->tree_radix, is_tree_multiroot,
                                   sim_coll_target, ctx);
            break;

        case COLLECTIVE_TOPOLOGY_KNOMIAL_MULTIROOT_TREE:
            is_tree_multiroot = 1;
        case COLLECTIVE_TOPOLOGY_KNOMIAL_TREE:
            res = send_knomial_tree(ctx->spec->proc_total_size, rank,
                                    ctx->spec->tree_radix, is_tree_multiroot,
                                    sim_coll_target, ctx);
            break;

        case COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING:
            res = send_recursive_k_ing(ctx->spec->proc_total_size, rank,
                                       ctx->spec->tree_radix, is_tree_multiroot,
                                       sim_coll_target, ctx);
            break;
        default:
            break; // the rest are not planned
        }

        if (res != OK) {
            return res;
        }

        plan = ctx->plans[local_rank]; // may have been reallocated by trees
        plan->peer_count = plan->peer_next;
        plan->peer_next = 0;
    }
    return res;
}

int sim_coll_ctx_alloc(collective_spec_t *spec,
                       collective_iteration_ctx_t **ret_ctx)
{
    collective_iteration_ctx_t *ctx;
    ctx = malloc(sizeof(*ctx));
    if (ctx == NULL) {
        return ERROR;
    }

    ctx->spec = spec;
    ctx->new_matrix = malloc(CTX_MATRIX_SIZE(ctx));
    if (ctx->new_matrix == NULL)
    {
        goto free_ctx;
    }

    ctx->old_matrix = malloc(CTX_MATRIX_SIZE(ctx));
    if (ctx->old_matrix == NULL)
    {
        goto free_new_matrix;
    }

#ifdef MPI_SPLIT_PROCS
    ctx->packets = malloc(spec->proc_group_size * sizeof(*ctx->packets));
    if (ctx->packets == NULL)
    {
        goto free_old_matrix;
    }

    ctx->targets = malloc(spec->proc_group_count * sizeof(*ctx->targets));
    if (ctx->targets == NULL)
    {
        goto free_packets;
    }
#endif

    if (spec->topology < COLLECTIVE_TOPOLOGY_RANDOM_PURE) {
        if (sim_coll_plan(ctx)) {
            goto free_pre_plan;
        }
    } else {
        ctx->plans = NULL;
    }

    ctx->time_offset = NULL;
    ctx->stored = 0;
    *ret_ctx = ctx;
    return OK;

free_pre_plan:
#ifdef MPI_SPLIT_PROCS
    free(ctx->targets);
free_packets:
    free(ctx->packets);
free_old_matrix:
#endif
    free(ctx->old_matrix);
free_new_matrix:
    free(ctx->new_matrix);
free_ctx:
    free(ctx);

    PERROR("Allocation failed!\n");
    return ERROR;
}

void sim_coll_ctx_free(collective_spec_t *spec)
{
    collective_iteration_ctx_t *ctx = spec->ctx;

#ifdef MPI_SPLIT_PROCS
    free(ctx->targets);
    free(ctx->packets);
#endif

    if (ctx->plans) {
        unsigned i;
        for (i = 0; i < ctx->planned; i++) {
            free(ctx->plans[i]);
        }
        free(ctx->plans);
        ctx->plans = NULL;
    }

    if (ctx->time_offset) {
        free(ctx->time_offset);
    }

    if (ctx->stored) {
        free(ctx->storage);
    }

    free(ctx->old_matrix);
    free(ctx->new_matrix);
    free(ctx);
}

int sim_coll_ctx_reset(collective_iteration_ctx_t *ctx)
{
    unsigned index;
    memset(ctx->new_matrix, 0, CTX_MATRIX_SIZE(ctx));
    memset(ctx->old_matrix, 0, CTX_MATRIX_SIZE(ctx));

    ctx->spec->random_up = 0;
    ctx->spec->random_down = 0;

    if (ctx->spec->model == COLLECTIVE_MODEL_PACKET_DELAY) {
        if (ctx->stored) {
            collective_datagram_t *slot = ctx->storage;
            unsigned slot_size = sizeof(*slot) + CTX_BITFIELD_SIZE(ctx);
            for (index = 0; index < ctx->stored;
                 index++, slot = (collective_datagram_t*)((char*)slot + slot_size)) {
                slot->delay = 0;
            }
        } else {
            ctx->stored = 1;
            ctx->storage = malloc(sizeof(*ctx->storage) + CTX_BITFIELD_SIZE(ctx));
            if (ctx->storage == NULL) {
                PERROR("Allocation Failed!\n");
                return ERROR;
            }
            ctx->storage[0].delay = 0; // mark vacant
        }
    } else if (ctx->stored) {
        free(ctx->storage);
        ctx->stored = 0;
    }

    if (ctx->spec->model == COLLECTIVE_MODEL_TIME_OFFSET) {
        if (ctx->time_offset == NULL) {
            ctx->time_offset = malloc(ctx->spec->proc_group_size * sizeof(unsigned));
            if (ctx->time_offset == NULL) {
                PERROR("Allocation Failed!\n");
                return ERROR;
            }
        }

        for (index = 0; index < ctx->spec->proc_group_size; index++)
        {
            if (ctx->time_offset) {
                ctx->time_offset[index] = CYCLIC_RANDOM(ctx->spec,
                                                        ctx->spec->offset_max);
            }
        }
    } else if (ctx->time_offset) {
        free(ctx->time_offset);
        ctx->time_offset = NULL;
    }

#ifdef MPI_SPLIT_PROCS
    ctx->packet_count = 0;
    memset(ctx->targets, 0,
           ctx->spec->proc_group_count * sizeof(*ctx->targets));
#endif

    // fill the initial bits
    SET_UNUSED_BITS(ctx);
    for (index = 0; index < ctx->spec->proc_group_size; index++)
    {
        SET_BIT(ctx, index, index +
                ctx->spec->proc_group_size * ctx->spec->proc_group_index);
    }

    // reset plans (for next topology)
    if (ctx->plans) {
        unsigned i;
        for (i = 0; i < ctx->planned; i++) {
            free(ctx->plans[i]);
        }
        free(ctx->plans);
        ctx->plans = NULL;
    }

    if (ctx->spec->topology < COLLECTIVE_TOPOLOGY_RANDOM_PURE) {
        return sim_coll_plan(ctx);
    }

    return OK;
}

static int sim_coll_iteration(collective_iteration_ctx_t *ctx)
{
    int ret_val;

    /* Switch step matrix before starting next iteration */
    memcpy(ctx->old_matrix, ctx->new_matrix, CTX_MATRIX_SIZE(ctx));

    /* One iteration on each local node */
    ctx->my_local_rank = 0;
    ctx->my_rank = ctx->spec->proc_group_index * ctx->spec->proc_group_size;
    while (ctx->my_local_rank < ctx->spec->proc_group_size)
    {
        /* calculate next target */
        ret_val = sim_coll_process(ctx);
        if (ret_val == ERROR)
        {
            return ERROR;
        }

        ctx->my_local_rank++;
        ctx->my_rank++;
    }

#ifdef MPI_SPLIT_PROCS
    if (ctx->spec->proc_group_count > 1) {
        ret_val = sim_coll_mpi_exchange(ctx);
        if (ret_val == ERROR) {
            return ERROR;
        }
        return (ret_val == OK);
    }
#endif
    return IS_ALL_FULL(ctx);
}

int sim_coll_once(collective_spec_t *spec)
{
    int ret_val = OK;

    if (spec->last_proc_total_size != spec->proc_total_size) {
        if (spec->ctx != NULL) {
            sim_coll_ctx_free(spec);
        }

        ret_val = sim_coll_ctx_alloc(spec, &spec->ctx);
        if (ret_val != OK) {
            return ret_val;
        }

        spec->last_proc_total_size = spec->proc_total_size;
    }

    if (sim_coll_ctx_reset(spec->ctx)) {
        return ERROR;
    }

    if (spec->step_count)
    {
        while ((spec->step_index < spec->step_count) && (!ret_val))
        {
            ret_val = sim_coll_iteration(spec->ctx);
            spec->step_index++;
        }
    }
    else
    {
        while (ret_val == OK)
        {
            ret_val = sim_coll_iteration(spec->ctx);
            spec->step_index++;
        }
    }

    return (ret_val == ERROR) ? ERROR : OK;
}

/*****************************************************************************\
 *                                                                           *
 *                              Collective Measurements                      *
 *                                                                           *
\*****************************************************************************/

void sim_coll_stats_init(struct stats *stats)
{
    memset(stats, 0, sizeof(*stats));
}

void sim_coll_stats_calc(struct stats *stats, unsigned long value)
{
    stats->cnt++;
    stats->sum += value;

    if (stats->max < value) {
        stats->max = value;
    }

    if ((stats->min > value) || (stats->min == 0)) {
        stats->min = value;
    }
}

#ifdef MPI_SPLIT_TESTS
void sim_coll_stats_aggregate(struct stats *stats, int is_root)
{
    if (is_root) {
        MPI_Reduce(MPI_IN_PLACE, &stats->cnt, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->sum, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->min, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->max, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    } else {
        MPI_Reduce(&stats->cnt, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->sum, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->min, 0, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->max, 0, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    }
}
#endif

void sim_coll_stats_print(struct stats *stats)
{
    printf(",%lu,%lu,%.2f", stats->min, stats->max, stats->avg);
}

int sim_coll_run(collective_spec_t *spec)
{
    int ret_val = OK;
    unsigned test_count = spec->test_count;
    unsigned test_index = 0;

#ifdef MPI_SPLIT_TESTS
    int is_root = (spec->test_node_index == 0);
    int aggregate = 1;
#endif

    if ((spec->topology < COLLECTIVE_TOPOLOGY_RANDOM_PURE) &&
        (spec->model == COLLECTIVE_MODEL_ITERATIVE)) {
            test_count = 1;
#ifdef MPI_SPLIT_TESTS
            aggregate = 0;
#endif
    }

    sim_coll_stats_init(&spec->steps);
    sim_coll_stats_init(&spec->msgs);
    sim_coll_stats_init(&spec->data);


#ifdef MPI_SPLIT_TESTS
    if (is_root) {
        test_count = test_count / spec->test_node_count +
                test_count % spec->test_node_count;
        if (test_count == spec->test_count) {
            aggregate = 0;
        }
    } else {
        test_count = test_count / spec->test_node_count;
        if (test_count == 0) {
            aggregate = 0;
        }
    }
#endif

    while ((test_index < test_count) && (ret_val != ERROR))
    {
        spec->step_index = 0;
        spec->messages_counter = 0;
        spec->data_len_counter = 0;

        ret_val = sim_coll_once(spec);

        /* Correction for unused bits */
        if (spec->proc_total_size & 7) {
            spec->data_len_counter -= spec->messages_counter *
                (((CALC_BITFIELD_SIZE(spec->proc_total_size) - 1) << 3)
                        - spec->proc_total_size);
        }

        sim_coll_stats_calc(&spec->steps, (unsigned long)spec->step_index);
        sim_coll_stats_calc(&spec->msgs, spec->messages_counter);
        sim_coll_stats_calc(&spec->data, spec->data_len_counter);

        test_index++;
    }

#ifdef MPI_SPLIT_TESTS
    if (aggregate) {
        sim_coll_stats_aggregate(&spec->steps, is_root);
    }
#endif
    spec->steps.avg = (float)spec->steps.sum / spec->steps.cnt;

#if defined(MPI_SPLIT_PROCS) || defined(MPI_SPLIT_TESTS)
    if (aggregate) {
        sim_coll_stats_aggregate(&spec->msgs, is_root);
        sim_coll_stats_aggregate(&spec->data, is_root);
    }
#endif
    spec->msgs.avg = (float)spec->msgs.sum / spec->msgs.cnt;
    spec->data.avg = (float)spec->data.sum / spec->data.cnt;

#ifdef MPI_SPLIT_TESTS
    if ((is_root) && (spec->test_node_index == 0)) {
#else
    if (spec->proc_group_index == 0) {
#endif
        printf("%i,%i,%i,%i,%i,%i,%.1f,%i",
               spec->proc_total_size, spec->model, spec->topology,
               spec->tree_radix, spec->offset_max, spec->delay_max,
               spec->fail_rate, spec->test_count);
        sim_coll_stats_print(&spec->steps);
        sim_coll_stats_print(&spec->msgs);
        sim_coll_stats_print(&spec->data);
        printf("\t%i:%i", spec->random_up, spec->random_down);
        printf("\n");
    }

    return ret_val;
}

int sim_coll_tree_topology(collective_spec_t *spec)
{
    int ret_val = OK;
    unsigned radix;

    if (spec->tree_radix != 0) {
        return sim_coll_run(spec);
    }

    if (spec->topology == COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING) {
        spec->tree_radix = 2;
        ret_val = sim_coll_run(spec);
    } else {
        for (radix = 2; ((radix < 10) && (ret_val != ERROR)); radix++) {
            spec->tree_radix = radix;
            switch (spec->topology) {
            case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 1> random steps */
                spec->cycle_random = radix - 1;
                spec->cycle_const = 1;
                break;

            case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix> const steps */
                spec->cycle_random = 1;
                spec->cycle_const = radix;
                break;

            case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
            case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
                spec->cycle_random = radix;
                spec->cycle_const = 0;
                break;

            default:
                spec->cycle_random = 0;
                spec->cycle_const = 0;
                break;
            }
            ret_val = sim_coll_run(spec);
        }
    }

    spec->tree_radix = 0;
    return ret_val;
}

int sim_coll_topology(collective_spec_t *spec)
{
    int ret_val = OK;
    collective_topology_t index;

    if (spec->topology < COLLECTIVE_TOPOLOGY_ALL) {
        return (spec->topology != COLLECTIVE_TOPOLOGY_RANDOM_PURE) ?
                sim_coll_tree_topology(spec) : sim_coll_run(spec);
    }

    for (index = 0;
         ((index < COLLECTIVE_TOPOLOGY_ALL) && (ret_val != ERROR));
         index++) {
        spec->topology = index;
        ret_val = (spec->topology != COLLECTIVE_TOPOLOGY_RANDOM_PURE) ?
                sim_coll_tree_topology(spec) : sim_coll_run(spec);
    }

    spec->topology = COLLECTIVE_TOPOLOGY_ALL;
    return ret_val;
}

int sim_coll_model_packet_delay(collective_spec_t *spec)
{
    int ret_val = OK;
    unsigned index, base2;

    if (spec->delay_max != 0) {
        return sim_coll_topology(spec);
    }

    /* Calculate the upper limit as closest power of 2 to the square root */
    for (base2 = 1; base2 * base2 < spec->proc_total_size; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val != ERROR)); index <<= 1) {
        spec->delay_max = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->delay_max = 0;
    return ret_val;
}

int sim_coll_model_packet_drop(collective_spec_t *spec)
{
    int ret_val = OK;
    float index;

    if (spec->fail_rate != 0) {
        return sim_coll_topology(spec);
    }

    for (index = 0.1; ((index < 0.6) && (ret_val != ERROR)); index += 0.2) {
        spec->fail_rate = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->fail_rate = 0;
    return ret_val;
}

int sim_coll_model_time_offset(collective_spec_t *spec)
{
    int ret_val = OK;
    unsigned index, base2;

    if (spec->offset_max != 0) {
        return sim_coll_topology(spec);
    }

    /* Calculate the upper limit as closest power of 2 to the square root */
    for (base2 = 1; base2 * base2 < spec->proc_total_size; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val != ERROR)); index <<= 1) {
        spec->offset_max = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->offset_max = 0;
    return ret_val;
}

int sim_coll_model_vars(collective_spec_t *spec)
{
    switch (spec->model) {
    case COLLECTIVE_MODEL_ITERATIVE:
        return sim_coll_topology(spec);

    case COLLECTIVE_MODEL_PACKET_DELAY:
        return sim_coll_model_packet_delay(spec);

    case COLLECTIVE_MODEL_PACKET_DROP:
        return sim_coll_model_packet_drop(spec);

    case COLLECTIVE_MODEL_TIME_OFFSET:
        return sim_coll_model_time_offset(spec);

    default:
        PERROR("Unknown Model!\n");
        return ERROR;
    }
}

int sim_coll_model(collective_spec_t *spec)
{
    int ret_val = OK;
    collective_model_t index;

    if (spec->model < COLLECTIVE_MODEL_ALL) {
        return sim_coll_model_vars(spec);
    }

    for (index = 0;
         ((index < COLLECTIVE_MODEL_ALL) && (ret_val != ERROR));
         index++) {
        spec->model = index;
        ret_val = sim_coll_model_vars(spec);
    }

    spec->model = COLLECTIVE_MODEL_ALL;
    return ret_val;
}

int sim_coll_procs(collective_spec_t *spec)
{
    int ret_val = OK;
    collective_model_t index;
    unsigned orig_count = spec->proc_group_count;

    if (spec->proc_total_size != 0) {
        spec->proc_group_size = spec->proc_total_size / spec->proc_group_count;
        spec->proc_total_size = spec->proc_group_size * spec->proc_group_count;
        spec->bitfield_size = CALC_BITFIELD_SIZE(spec->proc_total_size);
        return sim_coll_model(spec);
    }

    for (index = 2;
         ((__builtin_clz(index)) && (ret_val != ERROR));
         index <<= 1) {

        if ((index < MULTIPROCESS_ABSOLUTE_THRESHOLD) ||
            (index < MULTIPROCESS_RELATIVE_THRESHOLD * orig_count)) {
            if (spec->proc_group_index != 0) {
                continue;
            }

            spec->proc_group_count = 1;
            spec->proc_group_size = index;
            spec->proc_total_size = index;
        } else {
            spec->proc_group_count = orig_count;
            spec->proc_group_size = index / orig_count;
            spec->proc_total_size = spec->proc_group_size * orig_count;
        }
        spec->bitfield_size = CALC_BITFIELD_SIZE(spec->proc_total_size);

        ret_val = sim_coll_model(spec);
    }

    spec->model = COLLECTIVE_MODEL_ALL;
    return ret_val;
}

/*****************************************************************************\
 *                                                                           *
 *                              Main and argument parsing                    *
 *                                                                           *
\*****************************************************************************/

const char HELP_STRING[] =
        "Collecive simulator, by Alex Margolin.\nOptional arguments:\n\n"
        "    -m|--model <collective-model>\n"
        "        0 - Iterative\n"
        "        1 - Non-uniform\n"
        "        2 - Packet drop\n"
        "        3 - Time-offset\n"
        "        4 - All of the above (default)\n\n"
        "    -t|--topology <collective-topology>\n"
        "        0 - N-array tree\n"
        "        1 - K-nomial tree\n"
        "        2 - N-array tree, multi-root\n"
        "        3 - K-nomial tree, multi-root\n"
        "        4 - Recursive K-ing\n"
        "        5 - Random\n"
        "        6 - Enhanced random\n"
        "        7 - All of the above (default)\n\n"
        "    -i|--iterations <iter-count> - Test iteration count (default: 1)\n"
        "    -p|--procs <proc-count> - Set Amount of processes to simulate (default: 20)\n"
        "    -r|--radix <tree-radix> - Set tree radix for tree-based topologies "
        "(default: iterate from 3 to 10)\n\n"
        "    -f|--fail-rate <percentage> - Set failure percentage for packet drop "
        "model (default: iterate from 0.1 to 0.6 in incements of 0.2)\n\n"
        "    -d|--distance-max <iterations> - Set maximum distance for Non-uniform "
                "model (default: iterate from 1 to 16 in powers of 2)\n\n"
        "    -o|--offset-max <iterations> - Set maximum offset for Time-offset "
        "model (default: iterate from 0 to procs in powers of 2)\n\n"
        "";

int sim_coll_parse_args(int argc, char **argv, collective_spec_t *spec)
{
    int c;

    while (1) {
        int option_index = 0;
        static struct option long_options[] = {
                {"model",          required_argument, 0, 'm' },
                {"topology",       required_argument, 0, 't' },
                {"procs",          required_argument, 0, 'p' },
                {"radix",          required_argument, 0, 'r' },
                {"fail-rate",      required_argument, 0, 'f' },
                {"distance-max",   required_argument, 0, 'd' },
                {"offset-max",     required_argument, 0, 'o' },
                {"iterations",     required_argument, 0, 'i' },
                {0,                0,                 0,  0  },
        };

        c = getopt_long(argc, argv, "hm:t:p:g:r:f:d:o:i:",
                long_options, &option_index);
        if (c == -1)
            break;

        switch (c) {
        case 0:
            printf("option %s", long_options[option_index].name);
            if (optarg)
                printf(" with arg %s", optarg);
            printf("\n");
            break;

        case 'm':
            spec->model = atoi(optarg);
            if (spec->model > COLLECTIVE_MODEL_ALL) {
                printf("Invalid argument for -m: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 't':
            spec->topology = atoi(optarg);
            if (spec->topology > COLLECTIVE_TOPOLOGY_ALL) {
                printf("Invalid argument for -t: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 'p':
            spec->proc_total_size = atoi(optarg);
            break;

        case 'r':
            spec->tree_radix = atoi(optarg);
            break;

        case 'f':
            spec->fail_rate = atof(optarg);
            if ((spec->fail_rate <= 0.0) || (spec->fail_rate >= 1.0)) {
                printf("Invalid argument for -f: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 'd':
            spec->delay_max = atoi(optarg);
            break;

        case 'o':
            spec->offset_max = atoi(optarg);
            break;

        case 'i':
            spec->test_count = atoi(optarg);
            break;


        case 'h':
        default:
            printf(HELP_STRING);
            return ERROR;
        }
    }

    if (optind < argc) {
        printf("Invalid extra arguments: ");
        while (optind < argc)
            printf("%s ", argv[optind++]);
        printf("\n");
        printf(HELP_STRING);
        return ERROR;
    }

    spec->proc_group_size = spec->proc_total_size /
            spec->proc_group_count;
    return OK;
}

int main(int argc, char **argv)
{
    int ret_val;

    /* Set the defaults */
    collective_spec_t spec = {
        .model = COLLECTIVE_MODEL_ALL,
        .topology = COLLECTIVE_TOPOLOGY_ALL,
        .tree_radix = 0,
        .test_count = 1,
        .random_seed = 1,
        .proc_group_index = 0,
        .proc_group_count = 1,
        .proc_total_size = 0,
        .delay_max = 0,
        .offset_max = 0,
        .fail_rate = 0.0,
        .last_proc_total_size = 0,
        .ctx = 0
    };

    if (sim_coll_parse_args(argc, argv, &spec))
    {
        return ERROR;
    }

#ifdef TEST_FIRST
    if (test_tree_implementation())
    {
        PERROR("test_tree_implementation() failed!\n");
        return ERROR;
    }
#endif

#if defined(MPI_SPLIT_PROCS) || defined(MPI_SPLIT_TESTS)
    MPI_Init(&argc,&argv);
#endif

#if defined(MPI_SPLIT_TESTS)
    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.test_node_index);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.test_node_count);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    spec.random_seed += spec.test_node_index;
#endif

#if defined(MPI_SPLIT_PROCS)
    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.proc_group_index);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.proc_group_count);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    /* For reproducability - use only highest-power-of-2 first processes */
    if (__builtin_popcount(spec.proc_group_count) != 1) {
        if (spec.proc_group_index == 0) {
            PERROR("Process count (%i) is not a power of 2.",
                   spec.proc_group_count);
        }
        goto finalize;
    }

    spec.random_seed += spec.proc_group_index;
#endif

#ifdef MPI_SPLIT_TESTS
    if ((spec.proc_group_index == 0) &&
        (spec.test_node_index == 0)) {
#else
    if (spec.proc_group_index == 0) {
#endif
        printf("Execution specification:\n"
                "model=%i "
                "topology=%i "
                "step_count=%i "
                "test_count=%i "
                "tree_radix=%i "
                "random_seed=%i "
                "proc_group_index=%i "
                "proc_group_count=%i "
                "proc_group_size=%i\n",
                spec.model, spec.topology, spec.step_count, spec.test_count,
                spec.tree_radix, spec.random_seed, spec.proc_group_index,
                spec.proc_group_count, spec.proc_group_size);

        /* CSV header */
        printf("np,model,topo,radix,max_offset,max_delay,"
               "fails,runs,min_steps,max_steps,steps_avg,"
               "min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg\n");
    }

    ret_val = sim_coll_procs(&spec);

#ifdef MPI_SPLIT_TESTS
    if ((spec.proc_group_index == 0) &&
        (spec.test_node_index == 0)) {
#else
    if (spec.proc_group_index == 0) {
#endif
        if (ret_val == ERROR) {
            printf("Failure stopped the run!\n");
        } else {
            printf("Run completed successfully!\n");
        }
    }

    if (spec.ctx != NULL) {
        sim_coll_ctx_free(&spec);
    }

#if defined(MPI_SPLIT_PROCS) || defined(MPI_SPLIT_TESTS)
finalize:
    MPI_Finalize();
#endif

    return ret_val;
}
