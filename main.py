#!/usr/bin/env python3
import sys
import os
import shutil
import boto3
import logging
import pyarrow.parquet as pq

import dataset_partitioning
from dataset_partitioning.s3.iterator import S3DirectoryIterator
from dataset_partitioning.partition import ParquetBinPartitioner


def main(args):
    session = boto3.Session()

    s3 = session.resource('s3')
    # s3path = 's3://gbsc-aws-project-annohive-dev-user-krferrit-us-west-1/1000Orig-half2-parquet/'
    s3path = 's3://gbsc-aws-project-annohive-dev-user-krferrit-us-west-1/hg19_Variant_9B_Table_compressed/'

    bin_partitioner = ParquetBinPartitioner(s3, s3path)
    output_path = os.path.join('/mnt/ext', 'partitioned-directory/')
    shutil.rmtree(output_path, ignore_errors=True)
    os.mkdir(output_path)
    bin_partitioner.partition(62500, output_path)
    # bin_partitioner.partition(250000, output_path)

    # for file_path in S3DirectoryIterator(s3, s3path):
    #     print(file_path)

def main2(args):
    session = boto3.Session()
    s3 = session.resource('s3')

    if args[0] == 'getfiles':
        pass


if __name__ == '__main__':
    main(sys.argv[1:])
