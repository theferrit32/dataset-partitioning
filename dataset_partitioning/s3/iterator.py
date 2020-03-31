import boto3

from dataset_partitioning.logging_util import get_logger
logger = get_logger(__name__)

def parse_s3_path(path):
    if not path.startswith('s3://'):
        raise RuntimeError('Not an s3 path')
    else:
        path = path[len('s3://'):]
        if '/' in path:
            bucket_name = path[:path.index('/')]
            prefix = path[path.index('/')+1:]
            return (bucket_name, prefix)
        else:
            return (path, '')


class S3DirectoryIterator:
    def __init__(self, s3_client, s3_path, return_string=False):
        bucket_name, prefix = parse_s3_path(s3_path)
        self.bucket_name = bucket_name
        self.bucket = s3_client.Bucket(bucket_name)
        self.bucket.load()
        self.prefix = prefix
        self.return_string = return_string
        self.s3_client = s3_client
        logger.debug('__init__: bucket = %s, prefix = %s' % (bucket_name, prefix))

    def __iter__(self):
        self.bucket_object_list = []
        for bucket_obj in self.bucket.objects.filter(Prefix=self.prefix):
            # logger.debug('bucket_obj = ' + str(bucket_obj))
            self.bucket_object_list.append(bucket_obj)
        self.bucket_object_list_index = 0
        return self

    def __next__(self):
        if self.bucket_object_list_index >= len(self.bucket_object_list):
            raise StopIteration()
        bucket_obj = self.bucket_object_list[self.bucket_object_list_index]
        self.bucket_object_list_index += 1
        if self.return_string:
            return 's3://%s/%s' % (self.bucket_name, bucket_obj.key)
        else:
            # NOTE despite bucket_obj being returned from Bucket.objects,
            # it is not a s3.Object, it is an ObjectSummary, so convert
            return self.s3_client.Object(bucket_obj.bucket_name, bucket_obj.key)


class TestIterator:
    def __init__(self, start, end):
        self.val = start
        self.end = end

    def __iter__(self):
        return self

    def __next__(self):
        while self.val < self.end:
            yield self.val
            self.val += 1


if __name__ == '__main__':
    it = TestIterator(0, 100)
    counter = 0
    limit = 100
    for v in it:
        print(v)