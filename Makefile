all:
	~/workspace/ompi/build/bin/mpicc `find . -name '*.c'` -g -lm -o sim_allreduce
