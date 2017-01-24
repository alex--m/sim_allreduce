#include <mpi.h>
#include <stdio.h>
#include "state.h"

void stats_calc(struct stats *stats, unsigned long value)
{
    stats->cnt++;
    stats->sum += value;

    if (stats->max < value) {
        stats->max = value;
    }

    if ((stats->min > value) || (stats->min == 0)) {
        stats->min = value;
    }
}

void stats_aggregate(struct stats *stats, int is_root)
{
    if (is_root) {
        MPI_Reduce(MPI_IN_PLACE, &stats->cnt, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->sum, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->min, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->max, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
        stats->avg = (float)stats->sum / stats->cnt;
    } else {
        if (stats->min == 0) {
            stats->min = (unsigned long)-1;
        }
        MPI_Reduce(&stats->cnt, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->sum, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->min, 0, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->max, 0, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    }
}

void stats_print(struct stats *stats)
{
    printf(",%lu,%lu,%.2f", stats->min, stats->max, stats->avg);
}