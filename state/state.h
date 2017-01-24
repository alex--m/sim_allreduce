#include "../topology/topology.h"

typedef struct state state_t;

typedef struct stats {
    unsigned long cnt;
    unsigned long sum;
    unsigned long min;
    unsigned long max;
    float avg;
} stats_t;

typedef struct raw_stats {
	unsigned long step_counter;
	unsigned long messages_counter;
	unsigned long data_len_counter;
} raw_stats_t;

typedef struct exchange_optimization
{
	char *buf;
	int buf_len;
	int dtype_len;
	int *counts;
	int *displs;
} optimization_t;

void stats_calc(struct stats *stats, unsigned long value);

void stats_aggregate(struct stats *stats, int is_root);

void stats_print(struct stats *stats);

/* Create initial state for num_procs local, for a total of num_peer symetrical states */
int state_create(topology_spec_t *spec,
                 state_t *old_state,
                 state_t **new_state);

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(state_t *state,
                             void **sendbuf,
                             int **sendcounts,
                             int **sdispls,
                             unsigned long *total);

/* process a list of packets recieved from other peers (from MPI_Alltoallv) */
int state_process_next_step(state_t *state,
                            const char *incoming,
                            unsigned length);

/* Collect stats */
int state_get_raw_stats(state_t *state,
                        raw_stats_t *stats);

/* Destroy state and sum up stats */
void state_destroy(state_t *state);
