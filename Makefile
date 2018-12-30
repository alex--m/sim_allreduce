all:
	~/workspace/ompi/build/bin/mpicc `find . -name '*.c'` -lm -o sim_allreduce
