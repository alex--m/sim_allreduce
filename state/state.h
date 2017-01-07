#include "sim_allreduce.h"

typedef struct state state_t;

/* Create initial state for num_procs local, for a total of num_peer symetrical states */
int state_create(topology_iterator_t *initial, unsigned num_procs, unsigned num_peers, state_t *state_ctx);

/* generate a list of packets to be sent out to other peers (for MPI_Alltoallv) */
int state_generate_next_step(void **sendbuf, int **sendcounts, int **sdispls);

/* process a list of packets recieved from other peers (from MPI_Alltoallv) */
int state_generate_next_step(void *recvbuf, int *recvcounts, int *rdispls);

/* Destroy state and sum up stats */
void state_destroy(state_t *state_ctx, stats_t *stats);
