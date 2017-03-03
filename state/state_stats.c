#include <stdio.h>
#include "state.h"

static inline void stats_reduce_ulong(unsigned long *ptr, int is_root, MPI_Op op, MPI_Comm comm)
{
    if (is_root) {
        MPI_Reduce(MPI_IN_PLACE, ptr, 1, MPI_UNSIGNED_LONG, op, 0, comm);
    } else {
        MPI_Reduce(ptr, 0, 1, MPI_UNSIGNED_LONG, op, 0, comm);
    }
}

void stats_calc(struct stats *stats, unsigned long value, int is_root, MPI_Comm comm)
{
	if (comm) {
		stats_reduce_ulong(&value, is_root, MPI_SUM, comm);
		if (!is_root) {
			return;
		}
	}

    stats->sum += value;
    stats->cnt++;

    if (stats->max < value) {
        stats->max = value;
    }

    if ((stats->min > value) || (stats->min == 0)) {
        stats->min = value;
    }
}

void stats_aggregate(struct stats *stats, int is_root, MPI_Comm comm)
{
    if ((!is_root) && (stats->min == 0)) {
        stats->min = (unsigned long)-1;
    }

	stats_reduce_ulong(&stats->cnt, is_root, MPI_SUM, comm);
	stats_reduce_ulong(&stats->sum, is_root, MPI_SUM, comm);
	stats_reduce_ulong(&stats->min, is_root, MPI_MIN, comm);
	stats_reduce_ulong(&stats->max, is_root, MPI_MAX, comm);
}

void stats_print(struct stats *stats)
{
	stats->avg = stats->sum / stats->cnt;
    printf(",%lu,%lu,%.2f", stats->min, stats->max, stats->avg);
}
