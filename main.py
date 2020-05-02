#!/usr/bin/env python3
import sys
import os
import shutil
import boto3
import logging
import pyarrow.parquet as pq
import argparse

import dataset_partitioning
from dataset_partitioning.s3.iterator import S3DirectoryIterator
from dataset_partitioning.partition import ParquetBinPartitioner


def main(args):
    parser = argparse.ArgumentParser(description='dataset-partitioning main')
    parser.add_argument('s3bucket', type=str)
    opts = parser.parse_args(args)
    s3bucket = opts.s3bucket
    if not s3bucket.startswith('s3://'):
        s3bucket = 's3://' + s3bucket

    session = boto3.Session()

    s3 = session.resource('s3')
    s3path = s3bucket + '/hg19_Variant_9B_Table_compressed/'

    bin_partitioner = ParquetBinPartitioner(s3, s3path)
    output_path = os.path.join('/mnt/ext', 'partitioned-directory/')
    shutil.rmtree(output_path, ignore_errors=True)
    os.mkdir(output_path)
    bin_partitioner.partition(62500, output_path)
    # bin_partitioner.partition(250000, output_path)

    # for file_path in S3DirectoryIterator(s3, s3path):
    #     print(file_path)

if __name__ == '__main__':
    main(sys.argv[1:])

