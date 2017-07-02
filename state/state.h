#include <mpi.h>
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
    unsigned long last_step_counter;
    unsigned long first_step_counter;
    unsigned long messages_counter;
    unsigned long data_len_counter;
    unsigned long max_queueu_len;
} raw_stats_t;

void stats_calc(struct stats *stats, unsigned long value);

void stats_aggregate(struct stats *stats, int is_root);

void stats_print(struct stats *stats);

/* Create initial state for num_procs local, for a total of num_peer symetrical states */
int state_create(topology_spec_t *spec,
                 state_t *old_state,
                 state_t **new_state);

/* Run the next step */
int state_next_step(state_t *state);

/* Collect stats */
int state_get_raw_stats(state_t *state,
                        raw_stats_t *stats);

/* Destroy state and sum up stats */
void state_destroy(state_t *state);
