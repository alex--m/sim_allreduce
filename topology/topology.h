#include "sim_allreduce.h"

int topology_iterator_create(collective_topology_t topology, node_id node_count, topology_iterator_t *iterator);

node_id topology_iterator_next(topology_iterator_t *iterator);

int topology_iterator_omit(topology_iterator_t *iterator, node_id broken);

void topology_destroy();

int topology_test(collective_topology_t topology, node_id node_count);
