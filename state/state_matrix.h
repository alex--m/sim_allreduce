/*****************************************************************************\
 *                                                                           *
 *                              Bitfield Macros                              *
 *                                                                           *
\*****************************************************************************/

// Optimization: first bit is marked once full to avoid repeated checks

#define MOD_RES(x, y) ((x) + ((y) - ((x) % (y))))

#define CALC_BITFIELD_SIZE(size) MOD_RES(2 + ((size) >> 3), sizeof(unsigned))

#define CTX_BITFIELD_SIZE(ctx) ((ctx)->bitfield_size)

#define CTX_MATRIX_SIZE(ctx) \
    ((ctx)->spec->node_count * CTX_BITFIELD_SIZE(ctx))

#define GET_OLD_BITFIELD(ctx, local_node) ((ctx)->old_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define GET_NEW_BITFIELD(ctx, local_node) ((ctx)->new_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define _SET_BIT(bitfield, offset) \
	*(bitfield + (((offset) + 2) >> 3)) |= (1 << (((offset) + 2) & 7))

#define SET_OLD_BIT(ctx, local_node, node_bit) \
    _SET_BIT(GET_OLD_BITFIELD(ctx, local_node), node_bit)

#define SET_NEW_BIT(ctx, local_node, node_bit) \
    _SET_BIT(GET_NEW_BITFIELD(ctx, local_node), node_bit)

#define SET_FULL_HERE(bitfield) _SET_BIT(bitfield, -2)

#define _IS_BIT_SET_HERE(node_bit, bitfield) \
    ((*((bitfield) + (((node_bit) + 2) >> 3)) & \
        (1 << (((node_bit) + 2) & 7))) != 0)

#define IS_FULL_HERE(bitfield) _IS_BIT_SET_HERE(-2, bitfield)

#define IS_LIVE_HERE(bitfield) _IS_BIT_SET_HERE(-1, bitfield)

#define IS_BIT_SET_HERE(node_bit, bitfield) (IS_FULL_HERE(bitfield) || \
    _IS_BIT_SET_HERE(node_bit, bitfield))

#define IS_OLD_BIT_SET(ctx, local_node, node_bit) \
    IS_BIT_SET_HERE(node_bit, GET_OLD_BITFIELD(ctx, local_node))

#define IS_NEW_BIT_SET(ctx, local_node, node_bit) \
    IS_BIT_SET_HERE(node_bit, GET_NEW_BITFIELD(ctx, local_node))

#define SET_FULL(ctx, local_node) SET_NEW_BIT(ctx, local_node, -2)

#define IS_FULL(ctx, local_node) IS_NEW_BIT_SET(ctx, local_node, -2)

#define SET_LIVE(ctx, local_node) SET_NEW_BIT(ctx, local_node, -1)

#define UNSET_LIVE(ctx, local_node) \
    memset(GET_NEW_BITFIELD(ctx, local_node), 0, CTX_BITFIELD_SIZE(ctx))

#define IS_LIVE(ctx, local_node) IS_NEW_BIT_SET(ctx, local_node, -1)

#define IS_MINE_FULL(ctx) IS_FULL((ctx), ctx->my_rank)

#define IS_ALL_FULL(ctx) ({                                                   \
    int j = 0, is_full = 1;                                                   \
    while (is_full && (j < (ctx)->local_node_count)) {                        \
        is_full = IS_FULL(ctx, j);                                            \
        j++;                                                                  \
    }                                                                         \
    is_full;                                                                  \
})

#define POPCOUNT_HERE(bitfield, total_nodes) ({                               \
    unsigned i, cnt, max = CALC_BITFIELD_SIZE(total_nodes) / sizeof(unsigned);\
    if (IS_FULL_HERE(bitfield)) {                                             \
        cnt = total_nodes;                                                    \
    } else for (i = 0, cnt = 0; i < max; i++) {                               \
        cnt += __builtin_popcount(*((unsigned*)(bitfield) + i));              \
    }                                                                         \
    cnt - 1; /* Assume IS_LIVE */                                             \
})

#define POPCOUNT(ctx, local_node) \
    POPCOUNT_HERE(GET_NEW_BITFIELD(ctx, local_node), ctx->spec->node_count)

#define MERGE_HERE(present, addition, size, count) ({                         \
    if (IS_FULL_HERE(addition)) {                                             \
        SET_FULL_HERE(present);                                              \
    } else if (!IS_FULL_HERE(present)) {                                      \
        unsigned i, added, in_cnt, out_cnt, mask = (unsigned)-1;              \
        unsigned max = size / sizeof(unsigned);                               \
                                                                              \
        /* special treatment for first bits */                                \
        added = *((unsigned*)addition);                                       \
        *present |= added;                                                    \
        ((char*)&mask)[0] ^= 3; /* mask out full/live bits */                 \
        out_cnt = __builtin_popcount(*present & mask);                        \
        in_cnt = __builtin_popcount(added & mask);                            \
                                                                              \
        for (i = 1; i < max; i++)                                             \
        {                                                                     \
            added = *((unsigned*)(addition) + i);                             \
            *(present + i) |= added;                                          \
            out_cnt += __builtin_popcount(*(present + i));                    \
            in_cnt += __builtin_popcount(added);                              \
        }                                                                     \
        if (out_cnt == count) SET_FULL_HERE(present);                         \
    }                                                                         \
})

#define PRINT(ctx, local_node) ({                                             \
    int i;                                                                    \
    for (i = 0; i < (ctx)->spec->node_count; i++) {                           \
        printf("%i", IS_OLD_BIT_SET((ctx), local_node, i));                   \
    }                                                                         \
    printf(" (is_full=%i)", IS_FULL((ctx), local_node));                      \
    printf(" (is_live=%i)", IS_LIVE((ctx), local_node));                      \
})
