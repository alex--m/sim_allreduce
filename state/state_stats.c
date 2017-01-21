#include <stdio.h>
#include "state.h"

struct stats {
    unsigned long cnt;
    unsigned long sum;
    unsigned long min;
    unsigned long max;
    float avg;
};

void stats_init(struct stats *stats)
{
    memset(stats, 0, sizeof(*stats));
}

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

#ifdef MPI_SPLIT_TESTS
void stats_aggregate(struct stats *stats, int is_root)
{
    if (is_root) {
        MPI_Reduce(MPI_IN_PLACE, &stats->cnt, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->sum, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->min, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(MPI_IN_PLACE, &stats->max, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    } else {
        MPI_Reduce(&stats->cnt, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->sum, 0, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->min, 0, 1, MPI_UNSIGNED_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&stats->max, 0, 1, MPI_UNSIGNED_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    }
}
#endif

void stats_print(struct stats *stats)
{
    printf(",%lu,%lu,%.2f", stats->min, stats->max, stats->avg);
}
