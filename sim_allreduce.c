#include "state/state.h"
#include <mpi.h>

#define PERROR printf

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

	unsigned node_count; /* Total number of nodes */
	unsigned test_count; /* for statistical purposes */
    unsigned step_count; /* 0 to run until -1 is returned */
	unsigned last_node_total_size; /* OPTIMIZATION */

    unsigned mpi_rank;
    unsigned mpi_size;

	stats_t steps;
	stats_t data;
	stats_t msgs;
} sim_spec_t;

/*****************************************************************************\
 *                                                                           *
 *                              Collective Iterations                        *
 *                                                                           *
\*****************************************************************************/

int sim_test_iteration(sim_spec_t *spec, raw_stats_t *stats)
{
    int ret_val = OK;
    state_t *old_state = spec->state;

    /* invalidate cached "old state" if the process count has changed */
    if (spec->last_node_total_size != spec->node_count) {
        state_destroy(old_state);
        old_state = NULL;
    }
    spec->last_node_total_size = spec->node_count;

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
            ret_val = state_next_step(spec->state);
            spec->topology.step_index++;
        }
    }
    else
    {
    	/* Run until everybody completes (unlimited) */
        while  (ret_val == OK)
        {
            ret_val = state_next_step(spec->state);
            spec->topology.step_index++;

            if (spec->topology.step_index > 100) return ERROR;// TODO: remove!
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
    int is_root = (spec->mpi_rank == 0);

    /* no need to collect statistics on deterministic algorithms */
    if (spec->topology.model_type == COLLECTIVE_MODEL_BASE) {
            test_count = 1;
    }

    /* Distribute tests among processes */
    if (is_root) {
    	test_count = (test_count / spec->mpi_size) +
    			(test_count % spec->mpi_size);
    } else {
    	test_count /= spec->mpi_size;
    }

    /* Prepare for subsequent test iterations */
    spec->topology.node_count = spec->node_count;
    memset(&spec->steps, 0, sizeof(stats_t));
    memset(&spec->msgs, 0, sizeof(stats_t));
    memset(&spec->data, 0, sizeof(stats_t));

    while ((test_index < test_count) && (ret_val == OK))
    {
    	/* Run the a single iteration (independent) of the test */
        ret_val = sim_test_iteration(spec, &raw);

        /* Collect statistics */
        stats_calc(&spec->steps, raw.step_counter, is_root, MPI_COMM_WORLD);
        stats_calc(&spec->msgs, raw.messages_counter, is_root, MPI_COMM_WORLD);
        stats_calc(&spec->data, raw.data_len_counter, is_root, MPI_COMM_WORLD);

        test_index++;
    }

    /* If multiple tests were done on multiple processes - aggregate */
    if (spec->mpi_size > 1) {
        stats_aggregate(&spec->steps, is_root, MPI_COMM_WORLD);
        stats_aggregate(&spec->msgs, is_root, MPI_COMM_WORLD);
        stats_aggregate(&spec->data, is_root, MPI_COMM_WORLD);
    }

    if (is_root) {
        printf("%i,%i,%i,%i,%i,%.1f,%.1f,%i",
               spec->node_count,
               spec->topology.model_type,
               spec->topology.topology_type,
               spec->topology.topology.tree.radix,
			   (spec->topology.model_type == COLLECTIVE_MODEL_SPREAD) ?
			   		   spec->topology.model.max_spread : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_NODES_MISSING) ?
					   spec->topology.model.node_fail_rate : 0,
			   (spec->topology.model_type == COLLECTIVE_MODEL_NODES_FAILING) ?
					   spec->topology.model.node_fail_rate : 0,
			   aggregate ? spec->test_count : test_count);
        stats_print(&spec->steps);
        stats_print(&spec->msgs);
        stats_print(&spec->data);
        if (ret_val != OK) {
        	printf(" - ERROR!");
        }
        printf("\n");
    }

    return ret_val;
}

/*****************************************************************************\
 *                                                                           *
 *                              Collective Variables                         *
 *                                                                           *
\*****************************************************************************/

int sim_coll_tree_recovery(sim_spec_t *spec)
{
    int ret_val = OK;
    tree_recovery_type_t index;

    if (spec->topology.topology.tree.recovery != COLLECTIVE_RECOVERY_ALL) {
        return sim_test(spec);
    }

	for (index = 0;
		 ((index < COLLECTIVE_RECOVERY_ALL) && (ret_val == OK));
		 index++) {
		spec->topology.topology.tree.recovery = index;
		ret_val = sim_test(spec);
	}

	spec->topology.topology.tree.recovery = COLLECTIVE_RECOVERY_ALL;
    return ret_val;
}

int sim_coll_radix_topology(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned radix;

    if (spec->topology.topology.tree.radix != 0) {
        return sim_test(spec);
    }

	for (radix = 2;
		 ((radix < 10) && (radix <= spec->node_count) && (ret_val == OK));
		 radix++) {
		if (spec->topology.topology_type < COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING) {
			spec->topology.topology.tree.radix = radix;
			ret_val = sim_coll_tree_recovery(spec);
		} else {
			spec->topology.topology.butterfly.radix = radix;
			ret_val = sim_test(spec);
		}
	}

	if (spec->topology.topology_type < COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING) {
		spec->topology.topology.tree.radix = 0;
	} else {
		spec->topology.topology.butterfly.radix = 0;
	}
    return ret_val;
}

int sim_coll_topology(sim_spec_t *spec)
{
    int ret_val = OK;
    topology_type_t index;

    if (spec->topology.topology_type < COLLECTIVE_TOPOLOGY_ALL) {
        return sim_coll_radix_topology(spec);
    }

    for (index = 0;
         ((index < COLLECTIVE_TOPOLOGY_ALL) && (ret_val == OK));
         index++) {
        spec->topology.topology_type = index;
        ret_val = sim_coll_radix_topology(spec);
    }

    spec->topology.topology_type = COLLECTIVE_TOPOLOGY_ALL;
    return ret_val;
}

int sim_coll_model_spread(sim_spec_t *spec)
{
    int ret_val = OK;
    unsigned index, base2;

    if (spec->topology.model.max_spread != 0) {
        return sim_coll_topology(spec);
    }

    /* Calculate the upper limit as closest power of 2 to the square root */
    for (base2 = 1; base2 * base2 < spec->node_count; base2 = base2 * 2);

    for (index = 1; ((index <= base2) && (ret_val == OK)); index <<= 1) {
    	spec->topology.model.max_spread = index;
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.max_spread = 0;
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

    spec->topology.model.node_fail_rate = 0;
    return ret_val;
}

int sim_coll_model_vars(sim_spec_t *spec)
{
    switch (spec->topology.model_type) {
    case COLLECTIVE_MODEL_BASE:
        return sim_coll_topology(spec);

    case COLLECTIVE_MODEL_SPREAD:
        return sim_coll_model_spread(spec);

    case COLLECTIVE_MODEL_NODES_MISSING:
    	return sim_coll_model_nodes_missing(spec);

    case COLLECTIVE_MODEL_NODES_FAILING:
    	return sim_coll_model_nodes_missing(spec);

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
		"        1 - Fixed distance\n"
		"        2 - Random distance (uniform distribution)\n"
		"        3 - Random time-offset (\"spread\", uniform distribution)\n"
		"        4 - Missing nodes (\"inactive\", uniform distribution))\n"
		"        5 - Failing nodes (\"online failure\", uniform distribution))\n"
        "        6 - All of the above (default)\n\n"
        "    -t|--topology <collective-topology>\n"
        "        0 - N-array tree\n"
        "        1 - K-nomial tree\n"
        "        2 - N-array tree, multi-root\n"
        "        3 - K-nomial tree, multi-root\n"
        "        4 - Recursive K-ing\n"
        "        5 - All of the above (default)\n\n"
        "    -i|--iterations <iter-count> - Test iteration count (default: 1)\n"
        "    -p|--procs <proc-count> - Set Amount of processes to simulate (default: 20)\n"
        "    -r|--radix <tree-radix> - Set tree radix for tree-based topologies"
        " (default: iterate from 3 to 10)\n\n"
		"    -c|--recovery <recovery-method> - Set the method for tree fault recovery:"
        "        0 - Fall back to fathers, up the tree\n"
        "        1 - Fall back to brothers, across the tree\n"
        "        2 - All of the above (default)\n\n"
        "    -l|--latency <iterations> - Set the message delivery latency (default: 10)\n\n"
        "    -s|--max-spread <iterations> - Set maximum spread between processes"
        " (default: iterate from 0 to procs in powers of 2)\n\n"
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
				{"recovery",       required_argument, 0, 'c' },
                {"fail-rate",      required_argument, 0, 'f' },
                {"latency",        required_argument, 0, 'l' },
                {"max-spread",     required_argument, 0, 's' },
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
            spec->node_count = atoi(optarg);
            break;

        case 'r':
            spec->topology.topology.tree.radix = atoi(optarg);
            break;

        case 'l':
        	spec->topology.latency = atoi(optarg);
            break;

        case 's':
        	spec->topology.model.max_spread = atoi(optarg);
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

    return OK;
}

int main(int argc, char **argv)
{
    int ret_val;

    /* Set the defaults */
    sim_spec_t spec = {0};
    spec.topology.verbose = 0;
    spec.topology.latency = 10;
    spec.topology.model_type = COLLECTIVE_MODEL_ALL;
    spec.topology.topology_type = COLLECTIVE_TOPOLOGY_ALL;
    spec.test_count = DEFAULT_TEST_COUNT;

    MPI_Init(&argc, &argv);

    ret_val = MPI_Comm_rank(MPI_COMM_WORLD, (int*)&spec.mpi_rank);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    ret_val = MPI_Comm_size(MPI_COMM_WORLD, (int*)&spec.mpi_size);
    if (ret_val != MPI_SUCCESS) {
        goto finalize;
    }

    spec.topology.random_seed += spec.mpi_rank;
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

    if (spec.mpi_rank == 0) {
        printf("Execution specification:\n"
                "model=%i "
                "topology=%i "
                "step_count=%i "
                "test_count=%i "
                "tree_radix=%i "
                "latency=%i"
                "random_seed=%i\n",
                spec.topology.model_type, spec.topology.topology_type,
                spec.step_count, spec.test_count,
                spec.topology.topology.tree.radix,
				spec.topology.latency,
                spec.topology.random_seed);

        /* CSV header */
        printf("np,model,topo,radix,max_offset,max_delay,"
               "fails,runs,min_steps,max_steps,steps_avg,"
               "min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg\n");
    }

    if (spec.node_count) {
        ret_val = sim_coll_model(&spec);
    } else {
        unsigned nodes_log;
        for (nodes_log = 1;
             (nodes_log < (sizeof(unsigned)<<3)) && (ret_val == OK);
             nodes_log++) {
            spec.node_count = 1 << nodes_log;
            ret_val = sim_coll_model(&spec);
        }
    }

    if (spec.mpi_rank == 0) {
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
