import boto3

import dataset_partitioning
from dataset_partitioning.s3.iterator import S3DirectoryIterator

def main():
    session = boto3.Session()
    s3 = session.resource('s3')

    s3_root = ('s3://gbsc-aws-project-annohive-dev-user-krferrit-us-west-1'
        + '/1000Orig-half2-partitioned-4000/')

    replace_old = 'BIN_ID'
    replace_new = 'bin_id'

    for s3_object in S3DirectoryIterator(s3, s3_root):
        new_key = s3_object.key.replace(replace_old, replace_new)
        new_object = s3.Object(s3_object.bucket_name, new_key).copy({
            'Bucket': s3_object.bucket_name,
            'Key': s3_object.key
        })
        s3_object.delete()



if __name__ == '__main__':
    main()