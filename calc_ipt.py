#!/usr/bin/python

# This script does the math for an Idle Process Time (IPT) table
# Example run:
# calc_ipt.py 1024 1 10 100 200 300 400 500 600 700

import os
import sys
import csv
import subprocess
from io import StringIO

CMD = "./sim_allreduce --model 1 --latency 0 --reduce-only --iterations 1000 --topology {topo} --procs {procs} --radix {radix} --spread-mode {dist} --spread-avg {expected}"
OUTFILE = "reduce_{tree}_tree_{procs}_radix_{radix}_procs_{dist}_distribution_{expected}_expected_ipt"

TREES = {
    "kary" : 0,
    "knomial" : 1
}

TREE_RADIXES = (2, 3)

GAUSIAN_VARIANCE = 0.1

def calc_ipt(num_procs, tree, radix, is_gaussian_distribution, expected):
    # Check if results file exists
    file_args = {
        "tree" : tree,
        "radix" : radix,
        "procs" : num_procs,
        "dist" : ("uniform", "gaussian")[int(is_gaussian_distribution)],
        "expected" : expected
    }
    file_name = OUTFILE.format(**file_args)
    if (os.path.exists(file_name)):
        return

    # Generate the command-line
    cmd_args = {
        "topo" : TREES[tree],
        "radix" : radix,
        "procs" : num_procs,
        "dist" : int(is_gaussian_distribution),
        "expected" : expected
    }
    run_cmd = CMD.format(**cmd_args)

    # Run the calculation
    #print(run_cmd)
    out = subprocess.run(run_cmd.split(), capture_output=True, universal_newlines = True).stdout

    # Parse the output
    for row in csv.DictReader(StringIO(str(out))):
        wait_avg = float(row["wait_avg"])

    # write to a result file
    res = str(wait_avg / num_procs)
    print(res + " - " + file_name)
    open(file_name, "w").write(res)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        num_procs = int(sys.argv[1])
    else:
        print("USAGE: calc_ipt.py <#procs> [<E[T]>]")
        exit(1)

    for expected in sys.argv[2:]:
        for tree in TREES.keys():
            for radix in TREE_RADIXES:
                calc_ipt(num_procs, tree, radix, False, int(expected))
                calc_ipt(num_procs, tree, radix, True,  int(expected))

#alexm@alexm-laptop ~/workspace/sim_allreduce $ ./sim_allreduce --model 1 --latency 1 --reduce-only --topology 0 --iterations 1000 --procs 1024 --radix 3 --spread-mode 0 --spread-avg 10
#np,model,topo,radix,spread_mode,spread_avg,off-fail,on-fail,runs,min_steps,max_steps,steps_avg,min_in_spread,max_in_spread,in_spread_avg,min_out_spread,max_out_spread,out_spread_avg,min_msgs,max_msgs,msgs_avg,min_data,max_data,data_avg,min_queue,max_queue,queue_avg,min_dead,max_dead,dead_avg,min_wait,max_wait,wait_avg
#1024,1,0,3,0,10,0.00,0.00,1000,32,33,32.68,9,9,9.00,32,33,32.68,2,2,2.00,20488,21819,21075.44,4,7,5.08,0,0,0.00,571,657,614.33
