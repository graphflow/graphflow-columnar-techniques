#!/usr/bin/env python3

import os
import argparse
import subprocess

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    command = [ bin_home + 'benchmark-executor', '-i', args.input_graph_directory, '-r', str(args.runs),
        '-w', str(args.warmup_runs) ]
    benchmark_root = os.environ['GRAPHFLOW_HOME'] + '/benchmark/' + args.benchmark_query + '.query'
    command += ['-q', benchmark_root]

    print(' '.join(command))
    popen = subprocess.Popen(tuple(command), stdout=subprocess.PIPE)
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Benchmarks the Graphflow system on a query.')
    parser.add_argument('input_graph_directory', help='absolute path to the serialized input graph directory.')
    parser.add_argument('benchmark_query', help='Set of benchmark queries to be evaluated.')
    parser.add_argument('-r', '--runs', help='number of runs for each plan.', type=int, default=2)
    parser.add_argument('-w', '--warmup_runs', help='number of warmup runs.', type=int, default=1)
    return parser.parse_args()

if __name__ == '__main__':
    main()