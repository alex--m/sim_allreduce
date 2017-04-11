#include "state/state.h"
#include <mpi.h>

#define PERROR printf

#define PROCESS_NODE_CAPACITY (4096)
#define DEFAULT_TEST_COUNT (10)

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
    MPI_Comm mpi_comm;

    group_id node_group_index;     /* Index of this node group */
    group_id node_group_count;     /* Total number of groups */
	unsigned node_total_count;     /* Total number of nodes */
	unsigned last_node_total_size; /* OPTIMIZATION */

	unsigned test_count;       /* for statistical purposes */
    unsigned step_count;       /* 0 to run until -1 is returned */

	stats_t steps;
	stats_t data;
	stats_t msgs;
} sim_spec_t;

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
	unsigned long total;

	/* run the next step of this iteration of the test, generate output */
	ret_val = state_generate_next_step(spec->state, &sendbuf,
	        &sendcounts, &sdispls, &total);
	if (ret_val != OK) {
		return ret_val;
	}

	/* special case: localhost only */
	if (peer_count == 1) {
		return state_process_next_step(spec->state, sendbuf, total);
	}

	/* determine the sum of output packets on all peers waiting to be sent */
	ret_val = MPI_Alltoall(sendcounts, 1, MPI_INT,
						   recvcounts, 1, MPI_INT,
						   spec->mpi_comm);
	if (ret_val != OK) {
		return ret_val;
	}

	/* calculate the displacements for the next MPI_Alltoallv() */
	len = 0;
	rdispls[0] = 0;
	for (i = 0; i < (peer_count - 1); i++) {
		int next_size = recvcounts[i];
		rdispls[i + 1] = rdispls[i] + next_size;
		len += next_size;
	}
	len += recvcounts[i];

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
			                spec->mpi_comm);
	if (ret_val != OK) {
		return ret_val;
	}

	/* Use the incoming information to complete this step */
	ret_val = state_process_next_step(spec->state, recvbuf, len);
	MPI_Allreduce(MPI_IN_PLACE, &ret_val, 1, MPI_INT, MPI_LAND, spec->mpi_comm);
	return ret_val;
}

int sim_test_iteration(sim_spec_t *spec, raw_stats_t *stats)
{
    int ret_val = OK;
    state_t *old_state = spec->state;

    /* invalidate cached "old state" if the process count has changed */
    if (spec->last_node_total_size != spec->node_total_count) {
        state_destroy(old_state);
        old_state = NULL;

        spec->recv.counts = realloc(spec->recv.counts,
        		spec->node_group_count * sizeof(int));
        if (!spec->recv.counts) {
        	return ERROR;
        }

        spec->recv.displs = realloc(spec->recv.displs,
        		spec->node_group_count * sizeof(int));
        if (!spec->recv.displs) {
        	return ERROR;
        }
    }
    spec->last_node_total_size = spec->node_total_count;

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
            ret_val = sim_test_iteration_step(spec);
            spec->topology.step_index++;
        }
    }
    else
    {
    	/* Run until everybody completes (unlimited) */ // TODO: run until change to avg is < 0.1!!!
        while  (ret_val == OK)
        {
            ret_val = sim_test_iteration_step(spec);
            spec->topology.step_index++;
        }
    }

    if (ret_val == (-1)) { /* The only place that should compare explicitly to ERROR */
    	return ERROR;
    }

    ret_val = state_get_raw_stats(spec->state, stats);
    stats->step_counter = (unsigned long)spec->topology.step_index;
    return ret_val;
}

int sim_test(sim_spec_t *spec)
{
    int ret_val = OK;
    int aggregate = 1;
    raw_stats_t raw = {0};
    unsigned test_index = 0;
    unsigned test_count = spec->test_count;
    unsigned node_counter = spec->node_total_count;
    unsigned group_counter = node_counter / PROCESS_NODE_CAPACITY;
    unsigned orig_group_index = spec->node_group_index;
    unsigned orig_group_count = spec->node_group_count;
    int is_root = spec->node_group_index == 0;

    /* sanity check */
    if ((group_counter) && (node_counter % PROCESS_NODE_CAPACITY)) {
    	printf("\nERROR: Process count cannot be divided by %i!\n", PROCESS_NODE_CAPACITY);
    	exit(0);
    }

    /* required group check */
    if (group_counter > spec->node_group_count) {
    	printf("\nERROR: Insufficient nodes - need at least %i!\n", group_counter);
    	exit(0);
    }

    /* no need to collect statistics on deterministic algorithms */
    if ((spec->topology.topology_type < COLLECTIVE_TOPOLOGY_RANDOM_PURE) &&
        (spec->topology.model_type == COLLECTIVE_MODEL_ITERATIVE)) {
            test_count = 1;
            aggregate = 0;
    }

    /* Check if nodes exceed single process capacity */
    if (group_counter) {
        /* Distribute nodes among processes */
        if (spec->node_group_index < group_counter) {
            spec->topology.local_node_count = PROCESS_NODE_CAPACITY;
        } else {
            test_count = 0;
        }

        spec->node_group_count = group_counter;
        ret_val = MPI_Comm_split(MPI_COMM_WORLD,
                test_count, spec->node_group_index,
                &spec->mpi_comm);
        if (ret_val != MPI_SUCCESS) {
            return ERROR;
        }
    } else {
        /* Distribute tests among processes */
        if (spec->node_group_index) {
            test_count /= spec->node_group_count;
        } else {
            test_count = (test_count / spec->node_group_count)
                    + (test_count % spec->node_group_count);
        }
        spec->mpi_comm = MPI_COMM_WORLD;
        spec->topology.local_node_count = node_counter;
        spec->node_group_index = 0;
        spec->node_group_count = 1;
    }
    spec->topology.my_rank = spec->node_group_index;

    /* Prepare for subsequent test iterations */
    spec->topology.node_count = spec->node_total_count;
    memset(&spec->steps, 0, sizeof(stats_t));
    memset(&spec->msgs, 0, sizeof(stats_t));
    memset(&spec->data, 0, sizeof(stats_t));

    while ((test_index < test_count) && (ret_val == OK))
    {
    	/* Run the a single iteration (independent) of the test */
        ret_val = sim_test_iteration(spec, &raw);

        /* Collect statistics */
        stats_calc(&spec->steps, raw.step_counter, is_root, NULL);
        stats_calc(&spec->msgs, raw.messages_counter, is_root,
        		(group_counter > 1) ? spec->mpi_comm : NULL);
        stats_calc(&spec->data, raw.data_len_counter, is_root,
        		(group_counter > 1) ? spec->mpi_comm : NULL);

        test_index++;
    }

    /* If multiple tests were done on multiple processes - aggregate */
    if ((aggregate) && (group_counter > 1)) {
        stats_aggregate(&spec->steps, is_root, spec->mpi_comm);
        stats_aggregate(&spec->msgs, is_root, spec->mpi_comm);
        stats_aggregate(&spec->data, is_root, spec->mpi_comm);
    }

    if (orig_group_index == 0) {
        printf("%i,%i,%i,%i,%i,%i,%.1f,%.1f,%.1f,%i",
               spec->node_total_count,
               spec->topology.model_type,
               spec->topology.topology_type,
               spec->topology.topology.tree.radix,
			   (spec->topology.model_type == COLLECTIVE_MODEL_TIME_OFFSET) ?
			   		   spec->topology.model.time_offset_max : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_PACKET_DELAY) ?
					   spec->topology.model.packet_delay_max : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_PACKET_DROP) ?
					   spec->topology.model.packet_drop_rate : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_NODES_MISSING) ?
					   spec->topology.model.node_fail_rate : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_NODES_FAILING) ?
					   spec->topology.model.node_fail_rate : 0,
			   aggregate ? spec->test_count : test_count);
        stats_print(&spec->steps);
        stats_print(&spec->msgs);
        stats_print(&spec->data);
        printf("\n");
    }

    if (group_counter) {
        MPI_Comm_free(&spec->mpi_comm);
    }

    spec->node_group_index = orig_group_index;
    spec->node_group_count = orig_group_count;
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

	for (radix = 2;
		 ((radix < 10) && (radix <= spec->node_total_count) && (ret_val == OK));
		 radix++) {
		switch (spec->topology.topology_type) {
		case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_CONST: /* One const step for every <radix - 1> random steps */
			spec->topology.topology.random.cycle = radix - 1;
			break;

		case COLLECTIVE_TOPOLOGY_RANDOM_FIXED_RANDOM: /* One random step for every <radix> const steps */
			spec->topology.topology.random.cycle = radix;
			break;

		default:
			spec->topology.topology.tree.radix = radix;
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
         ((index < COLLECTIVE_TOPOLOGY_ALL) && (ret_val == OK));
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
    for (base2 = 1; base2 * base2 < spec->node_total_count; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val == OK)); index <<= 1) {
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

    for (index = 0.1; ((index < 0.6) && (ret_val == OK)); index += 0.2) {
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
    for (base2 = 1; base2 * base2 < spec->node_total_count; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val == OK)); index <<= 1) {
    	spec->topology.model.time_offset_max = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.time_offset_max = 0;
    return ret_val;
}

int sim_coll_model_nodes_missing(sim_spec_t *spec)
{
    int ret_val = OK;
    float index;

    if (spec->topology.model.node_fail_rate != 0) {
        return sim_coll_topology(spec);
    }

    for (index = 0.1; ((index < 0.6) && (ret_val == OK)); index += 0.2) {
    	spec->topology.model.node_fail_rate = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.packet_drop_rate = 0;
    return ret_val;
}

int sim_coll_model_nodes_failing(sim_spec_t *spec)
{
    int ret_val = OK;
    float index;

    if (spec->topology.model.node_fail_rate != 0) {
        return sim_coll_topology(spec);
    }

    for (index = 0.1; ((index < 0.1) && (ret_val == OK)); index += 0.01) {
    	spec->topology.model.node_fail_rate = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.packet_drop_rate = 0;
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

    case COLLECTIVE_MODEL_NODES_MISSING:
    	return sim_coll_model_nodes_missing(spec);

    case COLLECTIVE_MODEL_NODES_FAILING:
    	return sim_coll_model_nodes_failing(spec);

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
         ((index < COLLECTIVE_MODEL_ALL) && (ret_val == OK));
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

        c = getopt_long(argc, argv, "hvm:t:p:g:r:f:d:o:i:",
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
            spec->node_total_count = atoi(optarg);
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

        case 'v':
        	spec->topology.verbose = 1;
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

    spec->topology.local_node_count = spec->node_total_count
            / spec->node_group_count;
    return (spec->node_total_count % spec->node_group_count) ? ERROR : OK;
}

int main(int argc, char **argv)
{
    int ret_val;

    /* Set the defaults */
    sim_spec_t spec = {0};
    spec.topology.verbose = 0;
    spec.topology.model_type = COLLECTIVE_MODEL_ALL;
    spec.topology.topology_type = COLLECTIVE_TOPOLOGY_ALL;
    spec.test_count = DEFAULT_TEST_COUNT;

    MPI_Init(&argc, &argv);

    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.node_group_index);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.node_group_count);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    spec.topology.topology.random.random_seed += spec.node_group_index;
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

    if (spec.node_group_index == 0) {
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
                spec.topology.model_type, spec.topology.topology_type,
                spec.step_count, spec.test_count,
                spec.topology.topology.tree.radix,
                spec.topology.topology.random.random_seed,
                spec.node_group_index, spec.node_group_count,
                spec.node_group_count);

        /* CSV header */
        printf("np,model,topo,radix,max_offset,max_delay,"
               "fails,runs,min_steps,max_steps,steps_avg,"
               "min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg\n");
    }

    if (spec.node_total_count) {
        ret_val = sim_coll_model(&spec);
    } else {
        unsigned nodes_log;
        for (nodes_log = 1;
             (nodes_log < (sizeof(unsigned)<<3)) && (ret_val == OK);
             nodes_log++) {
            spec.node_total_count = 1 << nodes_log;
            ret_val = sim_coll_model(&spec);
        }
    }

    if (spec.node_group_index == 0) {
        if (ret_val != OK) {
            printf("Failure stopped the run!\n");
        } else {
            printf("Run completed successfully!\n");
        }
    }

finalize:
    MPI_Finalize();

    return ret_val;
}
