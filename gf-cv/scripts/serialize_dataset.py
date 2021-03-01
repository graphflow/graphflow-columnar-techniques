#!/usr/bin/env python3
import os
import subprocess
import argparse

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    dataset_serializer = [bin_home + 'dataset-serializer', '-i', args.input_directory, '-o', args.output_directory]
    popen = subprocess.Popen(
        tuple(dataset_serializer), stdout=subprocess.PIPE)
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='loads the csv files as a graph and serialize it.')
    parser.add_argument('input_directory',
        help='absolute path to the directory where dataset\'s metadata file is located.')
    parser.add_argument('output_directory',
        help='absolute path to the directory to keep serialized graph.')
    return parser.parse_args()

if __name__ == '__main__':
    main()

