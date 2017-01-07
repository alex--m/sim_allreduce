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
