#!/usr/bin/python
import sys
import math

L = 10
TREE_DIRECTIONS = 1

optimals = []
radixes = []
nomials = {}

hat_k = None
calc_hat = lambda k: (k != 1) and (max((k-1,L+1))+k-1) or 0

def optimal(t, k):
    if len(optimals) > t >= 0:
        return optimals[t]

    if t >= (hat_k+(L+2)*TREE_DIRECTIONS):
        res = optimal(t-2, k) + optimal(t-(L+2)*TREE_DIRECTIONS, k)
    elif t >= hat_k:
        res = k
    else:
        res = 0

    if t == len(optimals):
        optimals.append(res)
    return res

def nomial(x, k, h):
    # How many nodes are on the X-th level of a tree of height h
    if len(nomials.get(h, [])) > x >= 0:
        return nomials[h][x]

    if x >= h:
        res = 0
    elif x == 0:
        res = 1
    elif x < 0:
        res = 0
    else:
        res = (k-1)*sum(map(lambda y: nomial(x-1, k, y), range(h)))

    if x == len(nomials.get(h, [])):
        if x == 0:
            nomials[h] = [res]
        else:
            nomials[h].append(res)
    return res

def knomial(t, k):
    if t >= (hat_k+(L+2)*TREE_DIRECTIONS):
        t -= hat_k
        step = (L+k)*TREE_DIRECTIONS
        h, mod = t/step, t%step
        res = int(math.pow(k, h+1))
        if mod >= (L+2)*TREE_DIRECTIONS:
            extra_nodes = min(k-1, (mod-(2*L+4)+2)/2)
            res = res * (extra_nodes + 1)
        else:
            extra_nodes = 0
        for level in range(h+1):
            nodes_on_this_level = nomial(level, k, h)
            first_node_ready_at = TREE_DIRECTIONS*(level*(L+2) + (h-level)*(k-1)) + extra_nodes*2
            if t < (first_node_ready_at + (L+2)*TREE_DIRECTIONS):
                if t > (first_node_ready_at + (L+1)*TREE_DIRECTIONS):
                    res += 1
                break
            added_nodes = min(nodes_on_this_level, (t - first_node_ready_at - 2*L - 2)/2)
            res += k*added_nodes
            if added_nodes < nodes_on_this_level:
                break

    elif t >= hat_k:
        res = k
    else:
        res = 0
    return res

def radix(t, k):
    if len(radixes) > t >= 0:
        return radixes[t]

    if t >= (hat_k+(2*L)+4):
        res = radix(t-2, k) + radix(t-(L-2)*TREE_DIRECTIONS, k) - radix(t-TREE_DIRECTIONS*(L-k-1), k)
    elif t >= hat_k:
        res = k
    else:
        res = 0

    if t == len(radixes):
        radixes.append(res)
    return res

if __name__ == "__main__":
    if len(sys.argv) > 1:
        steps = None
        procs = int(sys.argv[1])
        if not procs and len(sys.argv) > 2:
            steps = int(sys.argv[2])
    else:
        print "USAGE: bounds.py <#procs> [<#steps-if-procs=0>]"
        exit(1)

    for k in range(1, 21):
        hat_k = calc_hat(k)

        step = 0
        radixes = []
        while True:
            print step, steps, radix(step, k)
            if (radix(step, k) >= procs) or (steps and step == steps):
                print "Optimal N-ary multi-root tree bound for radix=%i:\t%i steps." % (k, step)
                break
            step += 1

        step = 0
        optimals = []
        while True:
            if (optimal(step, k) >= procs) or (steps and step == steps):
                print "Optimal multi-root tree bound for radix=%i:\t\t%i steps." % (k, step)
                break
            step += 1

        step = 0
        nomials = {}
        while True:
            if (knomial(step, k) >= procs) or (steps and step == steps):
                print "K-nomial multi-root tree bound for radix=%i:\t\t%i steps." % (k, step)
                break
            step += 1
        print
