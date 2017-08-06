#include "state/state.h"
#include <mpi.h>
#include <math.h>

#define PERROR printf

#define DEFAULT_TEST_COUNT (1000)

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

typedef struct sim_spec
{
    state_t *state;
    topology_spec_t topology;

    node_id  node_count; /* Total number of nodes */
    unsigned test_count; /* for statistical purposes */
    step_num step_count; /* 0 to run until -1 is returned */
    node_id  last_node_total_size; /* OPTIMIZATION */

    unsigned mpi_rank;
    unsigned mpi_size;

    stats_t steps;
    stats_t in_spread;
    stats_t out_spread;
    stats_t data;
    stats_t msgs;
    stats_t queue;
    stats_t dead;
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
        spec->topology.node_count = spec->last_node_total_size;
        state_destroy(old_state);
        spec->topology.node_count = spec->node_count;
        old_state = NULL;
    }
    spec->last_node_total_size = spec->node_count;

    /* Create a new state for this iteration of the test */
    ret_val = state_create(&spec->topology, old_state, &spec->state);
    if (ret_val != OK) {
        return ret_val;
    }

    spec->topology.step_index = 0;
    if (spec->step_count) {
        /* Run <step_count> steps */
        while ((spec->topology.step_index < spec->step_count) && (!ret_val))
        {
            ret_val = state_next_step(spec->state);
            spec->topology.step_index++;
        }
    } else {
        /* Run until everybody completes (unlimited) */
        while (ret_val == OK) {
            ret_val = state_next_step(spec->state);
            spec->topology.step_index++;

            /* Sanity check: make sure we're not stuck indefinitely! */
            if ((spec->node_count * 1000 < spec->topology.step_index)) {
            	return ERROR;
            }
        }
    }

    if (ret_val == (-1)) { /* The only place that should compare explicitly to ERROR */
        return ERROR;
    }

    return state_get_raw_stats(spec->state, stats);
}

extern unsigned topology_max_offset;
int sim_test(sim_spec_t *spec)
{
    int ret_val = OK;
    raw_stats_t raw = {0};
    unsigned test_index;
    unsigned my_test_count;
    unsigned total_test_count = spec->test_count;
    int is_root = (spec->mpi_rank == 0);

    /* no need to collect statistics on deterministic algorithms */
    if (spec->topology.model_type == COLLECTIVE_MODEL_BASE) {
        total_test_count = 1;
    }

    /* Distribute tests among processes */
    if (is_root) {
        my_test_count = (total_test_count / spec->mpi_size) +
                (total_test_count % spec->mpi_size);
    } else {
        my_test_count = total_test_count / spec->mpi_size;
    }

    /* Prepare for subsequent test iterations */
    spec->topology.node_count = spec->node_count;
    memset(&spec->steps,      0, sizeof(stats_t));
    memset(&spec->in_spread,  0, sizeof(stats_t));
    memset(&spec->out_spread, 0, sizeof(stats_t));
    memset(&spec->msgs,       0, sizeof(stats_t));
    memset(&spec->data,       0, sizeof(stats_t));
    memset(&spec->queue,      0, sizeof(stats_t));
    memset(&spec->dead,       0, sizeof(stats_t));

    for (test_index = 0;
         ((test_index < my_test_count) && (ret_val == OK));
         test_index++)
    {
        /* Run the a single iteration (independent) of the test */
        ret_val = sim_test_iteration(spec, &raw);

        /* Collect statistics */
        stats_calc(&spec->steps,      raw.last_step_counter);
        stats_calc(&spec->in_spread,  topology_max_offset);
        stats_calc(&spec->out_spread, raw.last_step_counter - raw.first_step_counter);
        stats_calc(&spec->msgs,       raw.messages_counter);
        stats_calc(&spec->data,       raw.data_len_counter);
        stats_calc(&spec->queue,      raw.max_queueu_len);
        stats_calc(&spec->dead,       raw.death_toll);
        topology_max_offset = 0;
    }

    /* If multiple tests were done on multiple processes - aggregate */
    if (spec->mpi_size > 1) {
        stats_aggregate(&spec->steps,      is_root);
        stats_aggregate(&spec->in_spread,  is_root);
        stats_aggregate(&spec->out_spread, is_root);
        stats_aggregate(&spec->msgs,       is_root);
        stats_aggregate(&spec->data,       is_root);
        stats_aggregate(&spec->queue,      is_root);
        stats_aggregate(&spec->dead,       is_root);
    }

    if (is_root) {
        if (spec->topology.verbose) {
            printf("N=%lu M=%u Topo=%u Radix=%u Spread=%lu OfflineFail%%=%.2f OnlineFail%%=%.2f Steps(Avg.)=%lu (Out-)Spread(Avg.)=%lu Max-Queue-len=%lu\n",
                    spec->node_count,
                    spec->topology.model_type,
                    spec->topology.topology_type,
                    spec->topology.topology.tree.radix,
                    spec->topology.model.max_spread,
					spec->topology.model.offline_fail_rate,
					spec->topology.model.online_fail_rate,
					spec->steps.cnt ? spec->steps.sum / spec->steps.cnt : 0,
					spec->in_spread.cnt ? spec->in_spread.sum / spec->in_spread.cnt : 0,
					spec->queue.max);
        } else {
            printf("%lu,%u,%u,%u,%lu,%.2f,%.2f,%u",
                    spec->node_count,
                    spec->topology.model_type,
                    spec->topology.topology_type,
                    spec->topology.topology.tree.radix,
                    spec->topology.model.max_spread,
					spec->topology.model.offline_fail_rate,
					spec->topology.model.online_fail_rate,
                    total_test_count);
            stats_print(&spec->steps);
            stats_print(&spec->in_spread);
            stats_print(&spec->out_spread);
            stats_print(&spec->msgs);
            stats_print(&spec->data);
            stats_print(&spec->queue);
            stats_print(&spec->dead);
            if (ret_val != OK) {
                printf(" - ERROR!");
            }
            printf("\n");
        }
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
    tree_recovery_method_t index;

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
         ((radix < 2 * spec->topology.latency) &&
          (radix <= spec->node_count) &&
          (ret_val == OK));
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
         ((index < COLLECTIVE_TOPOLOGY_RECURSIVE_K_ING) && (ret_val == OK)); // TODO: reinstate RD
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

    if (spec->topology.model.offline_fail_rate != 0) {
        return sim_coll_topology(spec);
    }

    for (index = 1;
         ((index <= 100) &&
    	  (index < (spec->node_count / 10)) &&
		  (ret_val == OK));
         index *= 10) {
        spec->topology.model.offline_fail_rate = index;
    	printf("offline_fail_rate = %f\n", spec->topology.model.offline_fail_rate);
        ret_val = sim_coll_topology(spec);
    }

    spec->topology.model.offline_fail_rate = 0;
    return ret_val;
}

int sim_coll_model_nodes_failing(sim_spec_t *spec)
{
    int ret_val = OK;
    float index;

    if (spec->topology.model.online_fail_rate != 0) {
        return sim_coll_model_nodes_missing(spec);
    }

    for (index = 1;
         ((index <= 100) &&
    	  (index < (spec->node_count / 10)) &&
		  (ret_val == OK));
         index *= 10) {
        spec->topology.model.online_fail_rate = index;
        ret_val = sim_coll_model_nodes_missing(spec);
    }

    spec->topology.model.online_fail_rate = 0;
    return ret_val;
}

int sim_coll_model_vars(sim_spec_t *spec)
{
	int ret;
	step_num old_max_spread;
    switch (spec->topology.model_type) {
    case COLLECTIVE_MODEL_BASE:
        return sim_coll_topology(spec);

    case COLLECTIVE_MODEL_SPREAD:
        return sim_coll_model_spread(spec);

    case COLLECTIVE_MODEL_NODES_MISSING:
        return sim_coll_model_nodes_missing(spec);

    case COLLECTIVE_MODEL_NODES_FAILING:
        return sim_coll_model_nodes_failing(spec);

    case COLLECTIVE_MODEL_REAL:
    	old_max_spread = spec->topology.model.max_spread;
    	spec->topology.model.max_spread = sqrt(spec->node_count);
        ret = sim_coll_model_nodes_failing(spec);
        spec->topology.model.max_spread = old_max_spread;
        return ret;

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
				{"online-fails",   required_argument, 0, 'o' },
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

        case 'f':
        	spec->topology.model.offline_fail_rate = atof(optarg);
        	break;

        case 'o':
        	spec->topology.model.online_fail_rate = atof(optarg);
        	break;

        case 'i':
            spec->test_count = atoi(optarg);
            break;

        case 'v':
            spec->topology.verbose++;
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

    if (spec.mpi_rank == 0) {
        printf("Execution specification:\n"
                "model=%i "
                "topology=%i "
                "step_count=%lu "
                "test_count=%i "
                "tree_radix=%i "
                "latency=%lu "
                "random-seed=%i\n",
                spec.topology.model_type, spec.topology.topology_type,
                spec.step_count, spec.test_count,
                spec.topology.topology.tree.radix,
                spec.topology.latency,
                spec.topology.random_seed);

        if (!spec.topology.verbose) {
            /* CSV header */
            printf("np,model,topo,radix,max_offset,max_delay,fails,runs,"
                   "min_steps,max_steps,steps_avg,min_in_spread,max_in_spread,in_spread_avg,min_out_spread,max_out_spread,out_spread_avg,"
                   "min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg,min_queue,max_queue,queue_avg,"
            	   "min_dead,max_dead,dead_avg\n");
        }
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

    state_destroy(spec.state);

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

/* TODO: Fix memleaks: clear && clear && valgrind --leak-check=full ./Debug/sim_allreduce -p 8

==13748== 3,498,240 bytes in 145,760 blocks are definitely lost in loss record 230 of 230
          ---------------
==13748==    at 0x4C2DB8F: malloc (in /usr/lib/valgrind/vgpreload_memcheck-amd64-linux.so)
==13748==    by 0x400F53: comm_graph_create (comm_graph.c:46)
==13748==    by 0x40100C: comm_graph_clone (comm_graph.c:66)
==13748==    by 0x40232D: topology_iterator_omit (topo_iterator.c:113)
==13748==    by 0x404CED: state_process (state_ctx.c:218)
==13748==    by 0x404F32: state_dequeue (state_ctx.c:242)
==13748==    by 0x405024: state_next_step (state_ctx.c:270)
==13748==    by 0x405F10: sim_test_iteration (sim_allreduce.c:72)
==13748==    by 0x406131: sim_test (sim_allreduce.c:124)
==13748==    by 0x40650C: sim_coll_tree_recovery (sim_allreduce.c:201)
==13748==    by 0x4065AA: sim_coll_radix_topology (sim_allreduce.c:231)
==13748==    by 0x40666A: sim_coll_topology (sim_allreduce.c:259)


 */
