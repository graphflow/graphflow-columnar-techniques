#!/usr/bin/env python3
import os
import argparse
import subprocess

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    command = [ bin_home + 'benchmark-executor', '-i', args.input_graph_directory, '-r', str(args.runs),
                '-w', str(args.warmup_runs) ]
    if args.benchmark_set is not None:
        benchmark_root = os.environ['GRAPHFLOW_HOME'] + '/benchmark/' + args.benchmark_set
        command += ['-b', benchmark_root]
        if args.exclude_indices:
            command.append('-e')
        if args.files is not None:
            command.append('-f')
            command.extend(args.files)
    elif args.query_list_file is not None:
        command += ['-l', args.query_list_file]
    else: 
        raise Exception("either -b or -l has to be present.")
    print(' '.join(command))
    popen = subprocess.Popen(tuple(command), stdout=subprocess.PIPE)
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Benchmarks the Graphflow system on a set of queries.')
    parser.add_argument('input_graph_directory', help='absolute path to the serialized input graph directory.')
    parser.add_argument('-b', '--benchmark_set', help='Set of benchmark queries to be evaluated.')
    parser.add_argument('-r', '--runs', help='number of runs for each plan.', type=int, default=2)
    parser.add_argument('-w', '--warmup_runs', help='number of warmup runs.', type=int, default=1)
    parser.add_argument('-f', '--files', help='specify list of query files to benchmark.', nargs='+')
    parser.add_argument('-e', '--exclude_indices', help='exclude relevant indexes to be added to the dateset.', action="store_true")
    return parser.parse_args()

if __name__ == '__main__':
    main()