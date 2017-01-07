/*
 ============================================================================
 Name        : sim_coll.c
 Author      : Alex Margolin
 Version     : 1.0
 Description : Test Allreduce collective
 ============================================================================
 */

#include "comm_graph.h"
#include "sim_allreduce.h"

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

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
    unsigned delay_max;
    unsigned offset_max;
    unsigned failover;
    float fail_rate;

    unsigned proc_group_index;
    unsigned proc_group_count;
    unsigned proc_group_size;
    unsigned proc_total_size;
    unsigned bitfield_size;

    unsigned test_node_index;
    unsigned test_node_count;

    unsigned long messages_counter;
    unsigned long data_len_counter;

    struct stats steps;
    struct stats data;
    struct stats msgs;

    /* Optimization of buffer reuse, when possible */
    unsigned last_proc_total_size;
    struct collective_iteration_ctx *ctx;
} collective_spec_t;

/*****************************************************************************\
 *                                                                           *
 *                              MPI Mockup                                   *
 *                                                                           *
\*****************************************************************************/

#ifndef MPI
#define MPI_Comm void*
#define MPI_Datatype void*
#define MPI_COMM_WORLD NULL

int MPI_Init(int *argc, char ***argv)
{
	return OK;
}

int MPI_Alltoallv(const void *sendbuf, const int *sendcounts,
                  const int *sdispls, MPI_Datatype sendtype, void *recvbuf,
                  const int *recvcounts, const int *rdispls, MPI_Datatype recvtype,
                  MPI_Comm comm)
{
	return OK;
}

int MPI_Finalize()
{
	return OK;
}
#endif

/*****************************************************************************\
 *                                                                           *
 *                              Collective Iterations                        *
 *                                                                           *
\*****************************************************************************/

int sim_test_iteration_step(collective_spec_t *spec)
{
	void *sendbuf, *recvbuf = spec->optimize.recvbuf;
	int *sendcounts, *recvcounts = spec->optimize.recvcounts;
	int *sdispls, *rdispls = spec->optimize.rdispls;
	int ret_val;

	MPI_Datatype dtype = spec->optimize.mpi_dtype;

	ret_val = state_generate_next_step(&sendbuf, &sendcounts, &sdispls);
	if (ret_val != OK) {
		return ret_val;
	}

	ret_val = MPI_Alltoallv(sendbuf, sendcounts, sdispls, dtype,
			                recvbuf, recvcounts, rdispls, dtype,
				            MPI_COMM_WORLD);
	if (ret_val != OK) {
		return ret_val;
	}

	return state_process_next_step(recvbuf, recvcounts, rdispls);
}

int sim_test_iteration(collective_spec_t *spec)
{
    int ret_val = OK;

    if (spec->last_proc_total_size != spec->proc_total_size) {
        if (spec->ctx != NULL) {
            state_destroy(spec);
        }

        ret_val = state_create(spec, &spec->ctx);
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
    	/* Run <step_count> steps */
        while ((spec->step_index < spec->step_count) && (!ret_val))
        {
            ret_val = sim_coll_iteration_step(spec->ctx);
            spec->step_index++;
        }
    }
    else
    {
    	/* Run until everybody completes (unlimited) */
        while (ret_val == OK)
        {
            ret_val = sim_coll_iteration_step(spec->ctx);
            spec->step_index++;
        }
    }

    return (ret_val == ERROR) ? ERROR : OK;
}

int sim_test(collective_spec_t *spec)
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

/*****************************************************************************\
 *                                                                           *
 *                              Collective Variables                         *
 *                                                                           *
\*****************************************************************************/

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

int sim_calc_procs(collective_spec_t *spec)
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
