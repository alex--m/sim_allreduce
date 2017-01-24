/*****************************************************************************\
 *                                                                           *
 *                              Bitfield Macros                              *
 *                                                                           *
\*****************************************************************************/

// Optimization: first bit is marked once full to avoid repeated checks

#define CALC_BITFIELD_SIZE(size) (1 + ((size) >> 3) + ((size & 7) != 0))

#define CTX_BITFIELD_SIZE(ctx) ((ctx)->bitfield_size)

#define CTX_MATRIX_SIZE(ctx) \
    ((ctx)->local_node_count * CTX_BITFIELD_SIZE(ctx))

#define GET_OLD_BITFIELD(ctx, local_node) ((ctx)->old_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define GET_NEW_BITFIELD(ctx, local_node) ((ctx)->new_matrix + \
    ((local_node) * CTX_BITFIELD_SIZE(ctx)))

#define SET_BIT(ctx, local_node, node_bit) \
    *(GET_NEW_BITFIELD(ctx, local_node) + ((node_bit + 1) >> 3)) |= \
        (1 << ((node_bit + 1) & 7))

#define IS_BIT_SET_HERE(node_bit, bitfield) \
    ((*(bitfield + ((node_bit + 1) >> 3)) & \
        (1 << ((node_bit + 1) & 7))) != 0)

#define IS_BIT_SET(ctx, local_node, node_bit) \
    IS_BIT_SET_HERE(node_bit, GET_OLD_BITFIELD(ctx, local_node))

#define IS_MY_BIT_SET(ctx, node_bit) IS_BIT_SET(ctx, ctx->my_rank, node_bit)

#define IS_FULL(ctx, local_node) IS_BIT_SET(ctx, local_node, -1)

#define IS_MINE_FULL(ctx) IS_FULL((ctx), ctx->my_rank)

#define IS_ALL_FULL(ctx) ({                                                   \
    int j = 0, is_full = 1;                                                   \
    while (is_full && (j < (ctx)->local_node_count)) {                        \
        is_full = IS_FULL(ctx, j);                                            \
        j++;                                                                  \
    }                                                                         \
    is_full;                                                                  \
})

#define POPCOUNT(ctx, local_node) ({                                          \
    unsigned i, count = 0;                                                    \
    for (i = 0; i < (ctx)->local_node_count; i++) {                           \
        count += IS_BIT_SET((ctx), local_node, i);                            \
    }                                                                         \
    count;                                                                    \
})

#define MY_POPCOUNT(ctx) POPCOUNT(ctx, ctx->my_rank)

#define MERGE(ctx, local_proc, addition) ({                                   \
    unsigned i = 0, popcnt = 0, max = CTX_BITFIELD_SIZE(ctx);                 \
    unsigned char *present = GET_NEW_BITFIELD(ctx, local_proc);               \
    /*ctx->spec->messages_counter++;*/                                            \
    while (sizeof(unsigned) * (i + 1) < max)                                  \
    {                                                                         \
        unsigned added = *((unsigned*)(addition) + i);                        \
        *((unsigned*)present + i) |= added;                                   \
        popcnt += __builtin_popcount(added);                                  \
        i++;                                                                  \
    }                                                                         \
    /*ctx->spec->data_len_counter += popcnt;*/                                    \
    popcnt = 0;                                                               \
    i *= sizeof(unsigned);                                                    \
    while (i < max)                                                           \
    {                                                                         \
        unsigned char added = (addition)[i];                                  \
        popcnt = (popcnt << 8) | added;                                       \
        present[i] |= added;                                                  \
        i++;                                                                  \
    }                                                                         \
    /*ctx->spec->data_len_counter += */                                           \
        /*builtin_popcount(popcnt) - (((addition)[0]) == 1); */                  \
})

#define MERGE_LOCAL(ctx, local_proc, added_proc) \
    MERGE(ctx, local_proc, GET_OLD_BITFIELD(ctx, added_proc))

#define PRINT(ctx, local_node) ({                                             \
    int i;                                                                    \
    for (i = -1; i < (ctx)->local_node_count; i++) {                          \
        printf("%i", IS_BIT_SET((ctx), local_node, i));                       \
    }                                                                         \
})
