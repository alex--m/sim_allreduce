/*****************************************************************************\
 *                                                                           *
 *                              Bitfield Macros                              *
 *                                                                           *
\*****************************************************************************/

// Optimization: first bit is marked once full to avoid repeated checks

#define MOD_RES(x, y) ((x) + ((y) - ((x) % (y))))

#define CALC_BITFIELD_SIZE(size) MOD_RES(1 + ((size) >> 3), sizeof(unsigned))

#define CTX_BITFIELD_SIZE(ctx) ((ctx)->bitfield_size)

#define CTX_MATRIX_SIZE(ctx) \
    ((ctx)->spec->node_count * CTX_BITFIELD_SIZE(ctx))

#define GET_OLD_BITFIELD(ctx, local_node) ((ctx)->old_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define GET_NEW_BITFIELD(ctx, local_node) ((ctx)->new_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define _SET_BIT(bitfield, offset) \
	*(bitfield + (((offset) + 1) >> 3)) |= (1 << (((offset) + 1) & 7))

#define SET_BIT_HERE(bitfield, offset) \
	_SET_BIT(bitfield, offset)

#define SET_OLD_BIT(ctx, local_node, node_bit) \
    _SET_BIT(GET_OLD_BITFIELD(ctx, local_node), node_bit)

#define SET_NEW_BIT(ctx, local_node, node_bit) \
    _SET_BIT(GET_NEW_BITFIELD(ctx, local_node), node_bit)

#define SET_FULL_HERE(bitfield) _SET_BIT(bitfield, -1)

#define _IS_BIT_SET_HERE(node_bit, bitfield) \
    ((*((bitfield) + (((node_bit) + 1) >> 3)) & \
        (1 << (((node_bit) + 1) & 7))) != 0)

#define IS_FULL_HERE(bitfield) _IS_BIT_SET_HERE(-1, bitfield)

#define IS_BIT_SET_HERE(node_bit, bitfield) (IS_FULL_HERE(bitfield) || \
    _IS_BIT_SET_HERE(node_bit, bitfield))

#define IS_OLD_BIT_SET(ctx, local_node, node_bit) \
    IS_BIT_SET_HERE(node_bit, GET_OLD_BITFIELD(ctx, local_node))

#define IS_NEW_BIT_SET(ctx, local_node, node_bit) \
    IS_BIT_SET_HERE(node_bit, GET_NEW_BITFIELD(ctx, local_node))

#define SET_FULL(ctx, local_node) SET_NEW_BIT(ctx, local_node, -1)

#define IS_FULL(ctx, local_node) IS_NEW_BIT_SET(ctx, local_node, -1)

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
    cnt;                                                                      \
})

#define POPCOUNT(ctx, local_node) \
    POPCOUNT_HERE(GET_NEW_BITFIELD(ctx, local_node), ctx->spec->node_count)

#define MERGE(ctx, local_node, addition) ({                                   \
    if (IS_FULL_HERE(addition)) {                                             \
        SET_FULL(ctx, local_node);                                            \
    } else if (!IS_FULL(ctx, local_node)) {                                   \
        unsigned i, in_cnt = 0, out_cnt;                                      \
        unsigned *added = (unsigned*)addition;                                \
    	unsigned *present = (unsigned*)GET_NEW_BITFIELD(ctx, local_node);     \
        unsigned max = CTX_BITFIELD_SIZE(ctx) / sizeof(unsigned);             \
        for (i = 0, out_cnt = 0; i < max; i++, added++, present++)            \
        {                                                                     \
            in_cnt += __builtin_popcount(*added);                             \
            *present |= *added;                                               \
            out_cnt += __builtin_popcount(*present);                          \
        }                                                                     \
		if (in_cnt == 0) return ERROR;                                        \
		if (out_cnt == ctx->spec->node_count) SET_FULL(ctx, local_node);      \
    }                                                                         \
})

#define PRINT(ctx, local_node) ({                                             \
    int i;                                                                    \
    for (i = 0; i < (ctx)->spec->node_count; i++) {                           \
        printf("%i", IS_NEW_BIT_SET((ctx), local_node, i));                   \
    }                                                                         \
    printf(" (is_full=%i)", IS_FULL((ctx), local_node));                      \
})
