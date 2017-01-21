#include "state/state.h"

#if defined(MPI_SPLIT_PROCS) || defined(MPI_SPLIT_TESTS)
#include <mpi.h>
#endif

#define PERROR printf
#define VERBOSE_PROC_THRESHOLD (0)
#define MULTIPROCESS_ABSOLUTE_THRESHOLD (128)
#define MULTIPROCESS_RELATIVE_THRESHOLD (1)

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

typedef struct sim_spec
{
	state_t *state;
	topology_spec_t topology;
	optimization_t recv;

	unsigned test_count; /* for statistical purposes */
    unsigned step_count; /* 0 to run until -1 is returned */

	unsigned node_total_size;
	unsigned last_node_total_size; /* OPTIMIZATION */
} sim_spec_t;

/*****************************************************************************\
 *                                                                           *
 *                              MPI Mockup                                   *
 *                                                                           *
\*****************************************************************************/

#ifndef MPI
#define MPI_Comm void*
#define MPI_Datatype void*
#define MPI_COMM_WORLD NULL
#define MPI_SUCCESS 0
#define MPI_BYTE NULL
#define MPI_INT NULL

int MPI_Init(int *argc, char ***argv)
{
	return OK;
}

int MPI_Comm_rank(MPI_Comm comm, int* rank)
{
	return 0;
}

int MPI_Comm_size(MPI_Comm comm, int* rank)
{
	return 1;
}

int MPI_Alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm)
{
	return ERROR; /* Should never be called */
}

int MPI_Alltoallv(const void *sendbuf, const int *sendcounts,
                  const int *sdispls, MPI_Datatype sendtype, void *recvbuf,
                  const int *recvcounts, const int *rdispls, MPI_Datatype recvtype,
                  MPI_Comm comm)
{
	return ERROR; /* Should never be called */
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

enum split_mode {
	NO_SPLIT = 0,
	SPLIT_ITERATIONS, /* every node does a portion of the iterations */
	SPLIT_PROCS, /* every node simulates a portion of procs in each iteration */
};

int sim_test_iteration_step(sim_spec_t *spec)
{
	unsigned peer_count = spec->node_group_count;
	void *sendbuf, *recvbuf = spec->recv.buf;
	int *sendcounts, *recvcounts = spec->recv.counts;
	int *sdispls, *rdispls = spec->recv.displs;
	int len, ret_val, i;

	/* run the next step of this iteration of the test, generate output */
	ret_val = state_generate_next_step(spec->state, &sendbuf, &sendcounts, &sdispls);
	if (ret_val != OK) {
		return ret_val;
	}

	/* determine the sum of output packets on all peers waiting to be sent */
	ret_val = MPI_Alltoall(sendcounts, 1, MPI_INT,
						   recvcounts, 1, MPI_INT, MPI_COMM_WORLD);
	if (ret_val != OK) {
		return ret_val;
	}

	/* special case: localhost only */
	if (peer_count == 1) {
		return state_process_next_step(sendbuf, sendcounts, sdispls);
	}

	/* calculate the displacements for the next MPI_Alltoallv() */
	len = 0;
	rdispls[0] = 0;
	for (i = 0; i < peer_count; i++) {
		int next_size = recvcounts[i];
		rdispls[i + 1] = rdispls[i] + next_size;
		len += next_size;
	}

	/* ensure the incoming buffer is large enough for the exchange */
	if (spec->recv.buf_len < len) {
		recvbuf = realloc(recvbuf, len);
		if (!recvbuf) {
			return ERROR;
		}

		spec->recv.buf_len = len;
		spec->recv.buf = recvbuf;
	}

	/* exchange information between peers */
	ret_val = MPI_Alltoallv(sendbuf, sendcounts, sdispls, MPI_BYTE,
			                recvbuf, recvcounts, rdispls, MPI_BYTE,
				            MPI_COMM_WORLD);
	if (ret_val != OK) {
		return ret_val;
	}

	/* Use the incoming information to complete this step */
	return state_process_next_step(spec->state, recvbuf, len);
}

int sim_test_iteration(sim_spec_t *spec)
{
    int ret_val = OK;
    state_t *old_state = spec->state;

    /* invalidate cached "old state" if the process count has changed */
    if (old_state && (spec->last_node_total_size != spec->node_total_size)) {
        state_destroy(old_state);
    }
    spec->last_node_total_size = spec->node_total_size;

    /* Create a new state for this iteration of the test */
    ret_val = state_create(&spec->topology, old_state, &spec->state);
    if (ret_val != OK) {
        return ret_val;
    }

    spec->topology.step_index = 0;
    if (spec->step_count)
    {
    	/* Run <step_count> steps */
        while ((spec->topology.step_index < spec->step_count) && (!ret_val))
        {
            ret_val = sim_test_iteration_step(spec->state);
            spec->topology.step_index++;
        }
    }
    else
    {
    	/* Run until everybody completes (unlimited) */
        while (ret_val == OK)
        {
            ret_val = sim_test_iteration_step(spec->state);
            spec->topology.step_index++;
        }
    }

    return (ret_val == ERROR) ? ERROR : OK;
}

int sim_test(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned test_count = spec->test_count;
    unsigned test_index = 0;







    int is_root = (spec->test_node_index == 0);
    int aggregate = 1;

    if ((spec->topology.topology_type < COLLECTIVE_TOPOLOGY_RANDOM_PURE) &&
        (spec->topology.model_type == COLLECTIVE_MODEL_ITERATIVE)) {
            test_count = 1;
            aggregate = 0;
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

        ret_val = sim_test_iteration(spec);

        /* Correction for unused bits */
        if (spec->node_total_size & 7) {
            spec->data_len_counter -= spec->messages_counter *
                (((CALC_BITFIELD_SIZE(spec->node_total_size) - 1) << 3)
                        - spec->node_total_size);
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
    if (spec->node_group_index == 0) {
#endif
        printf("%i,%i,%i,%i,%i,%i,%.1f,%i",
               spec->node_total_size, spec->model, spec->topology,
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

int sim_coll_tree_topology(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned radix;

    if (spec->topology.topology.tree.radix != 0) {
        return sim_test(spec);
    }

	for (radix = 2; ((radix < 10) && (ret_val != ERROR)); radix++) {
		switch (spec->topology.topology_type) {
		case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 1> random steps */
			spec->topology.topology.random.cycle_random = radix - 1;
			spec->topology.topology.random.cycle_const = 1;
			break;

		case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix> const steps */
			spec->topology.topology.random.cycle_random = 1;
			spec->topology.topology.random.cycle_const = radix;
			break;

		case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_LINEAR: /* After every <radix> random steps - add one const step to the cycle */
		case COLLECTIVE_TOPOLOGY_RANDOM_VARIABLE_EXPONENTIAL: /* After every <radix> random steps - double the non-random steps in the cycle */
			spec->topology.topology.random.cycle_random = radix;
			spec->topology.topology.random.cycle_const = 0;
			break;

		default:
			spec->topology.topology.tree.radix = 0;
			break;
		}
		ret_val = sim_test(spec);
	}

    spec->topology.topology.tree.radix = 0;
    return ret_val;
}

int sim_coll_topology(sim_spec_t *spec)
{
    int ret_val = OK;
    topology_type_t index;

    if (spec->topology.topology_type < COLLECTIVE_TOPOLOGY_ALL) {
        return (spec->topology.topology_type != COLLECTIVE_TOPOLOGY_RANDOM_PURE) ?
                sim_coll_tree_topology(spec) : sim_test(spec);
    }

    for (index = 0;
         ((index < COLLECTIVE_TOPOLOGY_ALL) && (ret_val != ERROR));
         index++) {
        spec->topology.topology_type = index;
        ret_val = (spec->topology.topology_type != COLLECTIVE_TOPOLOGY_RANDOM_PURE) ?
                sim_coll_tree_topology(spec) : sim_test(spec);
    }

    spec->topology.topology_type = COLLECTIVE_TOPOLOGY_ALL;
    return ret_val;
}

int sim_coll_model_packet_delay(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned index, base2;

    if (spec->topology.model.packet_delay_max != 0) {
        return sim_coll_topology(spec);
    }

    /* Calculate the upper limit as closest power of 2 to the square root */
    for (base2 = 1; base2 * base2 < spec->node_total_size; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val != ERROR)); index <<= 1) {
    	spec->topology.model.packet_delay_max = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.packet_delay_max = 0;
    return ret_val;
}

int sim_coll_model_packet_drop(sim_spec_t *spec)
{
    int ret_val = OK;
    float index;

    if (spec->topology.model.packet_drop_rate != 0) {
        return sim_coll_topology(spec);
    }

    for (index = 0.1; ((index < 0.6) && (ret_val != ERROR)); index += 0.2) {
    	spec->topology.model.packet_drop_rate = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.packet_drop_rate = 0;
    return ret_val;
}

int sim_coll_model_time_offset(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned index, base2;

    if (spec->topology.model.time_offset_max != 0) {
        return sim_coll_topology(spec);
    }

    /* Calculate the upper limit as closest power of 2 to the square root */
    for (base2 = 1; base2 * base2 < spec->node_total_size; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val != ERROR)); index <<= 1) {
    	spec->topology.model.time_offset_max = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.time_offset_max = 0;
    return ret_val;
}

int sim_coll_model_vars(sim_spec_t *spec)
{
    switch (spec->topology.model_type) {
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

int sim_coll_model(sim_spec_t *spec)
{
    int ret_val = OK;
    model_type_t index;

    if (spec->topology.model_type < COLLECTIVE_MODEL_ALL) {
        return sim_coll_model_vars(spec);
    }

    for (index = 0;
         ((index < COLLECTIVE_MODEL_ALL) && (ret_val != ERROR));
         index++) {
    	spec->topology.model_type = index;
        ret_val = sim_coll_model_vars(spec);
    }

    spec->topology.model_type = COLLECTIVE_MODEL_ALL;
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

int sim_coll_parse_args(int argc, char **argv, sim_spec_t *spec)
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
            spec->topology.model_type = atoi(optarg);
            if (spec->topology.model_type > COLLECTIVE_MODEL_ALL) {
                printf("Invalid argument for -m: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 't':
            spec->topology.topology_type = atoi(optarg);
            if (spec->topology.topology_type > COLLECTIVE_TOPOLOGY_ALL) {
                printf("Invalid argument for -t: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 'p':
            spec->node_total_size = atoi(optarg);
            break;

        case 'r':
            spec->topology.topology.tree.radix = atoi(optarg);
            break;

        case 'f':
            spec->topology.model.packet_drop_rate = atof(optarg);
            if ((spec->topology.model.packet_drop_rate <= 0.0) ||
            	(spec->topology.model.packet_drop_rate >= 1.0)) {
                printf("Invalid argument for -f: %s\n%s", optarg, HELP_STRING);
                return ERROR;
            }
            break;

        case 'd':
        	spec->topology.model.packet_delay_max = atoi(optarg);
            break;

        case 'o':
        	spec->topology.model.time_offset_max = atoi(optarg);
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

    spec->node_group_size = spec->node_total_size /
            spec->node_group_count;
    return OK;
}

int sim_calc_procs(sim_spec_t *spec)
{
    int ret_val = OK;
    int index;
    unsigned orig_count = spec->node_group_count;

    if (spec->node_total_size != 0) {
        spec->node_group_size = spec->node_total_size / spec->node_group_count;
        spec->node_total_size = spec->node_group_size * spec->node_group_count;
        spec->bitfield_size = CALC_BITFIELD_SIZE(spec->node_total_size);
        return sim_coll_model(spec);
    }

    for (index = 2;
         ((__builtin_clz(index)) && (ret_val != ERROR));
         index <<= 1) {

        if ((index < MULTIPROCESS_ABSOLUTE_THRESHOLD) ||
            (index < MULTIPROCESS_RELATIVE_THRESHOLD * orig_count)) {
            if (spec->node_group_index != 0) {
                continue;
            }

            spec->node_group_count = 1;
            spec->node_group_size = index;
            spec->node_total_size = index;
        } else {
            spec->node_group_count = orig_count;
            spec->node_group_size = index / orig_count;
            spec->node_total_size = spec->node_group_size * orig_count;
        }
        spec->bitfield_size = CALC_BITFIELD_SIZE(spec->node_total_size);

        ret_val = sim_coll_model(spec);
    }

    spec->model = COLLECTIVE_MODEL_ALL;
    return ret_val;
}

int main(int argc, char **argv)
{
    int ret_val;

    /* Set the defaults */
    sim_spec_t spec = {0};
    spec.topology.model_type = COLLECTIVE_MODEL_ALL;
    spec.topology.topology_type = COLLECTIVE_TOPOLOGY_ALL;
    spec.test_count = 1;

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

    MPI_Init(&argc,&argv);

    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.topology.node_group_index);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.node_group_count);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    spec.random_seed += spec.test_node_index;
    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.node_group_index);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.node_group_count);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    /* For reproducability - use only highest-power-of-2 first processes */
    if (__builtin_popcount(spec.node_group_count) != 1) {
        if (spec.node_group_index == 0) {
            PERROR("Process count (%i) is not a power of 2.",
                   spec.node_group_count);
        }
        goto finalize;
    }

    spec.random_seed += spec.node_group_index;

    if ((spec.node_group_index == 0) &&
        (spec.test_node_index == 0)) {
        printf("Execution specification:\n"
                "model=%i "
                "topology=%i "
                "step_count=%i "
                "test_count=%i "
                "tree_radix=%i "
                "random_seed=%i "
                "node_group_index=%i "
                "node_group_count=%i "
                "node_group_size=%i\n",
                spec.model, spec.topology, spec.step_count, spec.test_count,
                spec.tree_radix, spec.random_seed, spec.node_group_index,
                spec.node_group_count, spec.node_group_size);

        /* CSV header */
        printf("np,model,topo,radix,max_offset,max_delay,"
               "fails,runs,min_steps,max_steps,steps_avg,"
               "min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg\n");
    }

    ret_val = sim_coll_procs(&spec);

    if ((spec.node_group_index == 0) &&
        (spec.test_node_index == 0)) {
        if (ret_val == ERROR) {
            printf("Failure stopped the run!\n");
        } else {
            printf("Run completed successfully!\n");
        }
    }

    if (spec.ctx != NULL) {
        sim_coll_ctx_free(&spec);
    }

finalize:
    MPI_Finalize();

    return ret_val;
}
