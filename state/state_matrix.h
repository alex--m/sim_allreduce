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
