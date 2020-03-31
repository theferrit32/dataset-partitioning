import os, shutil, binascii
import gc
import math
# import s3fs
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

from dataset_partitioning.s3.iterator import S3DirectoryIterator
from dataset_partitioning.logging_util import get_logger
from dataset_partitioning.util import chunk_list
from dataset_partitioning.merge_parquet import merge_partitioned_directory
logger = get_logger(__name__)

def rand_string(length):
    return binascii.hexlify(os.urandom(int(length/2 + 1))).decode('utf-8')[:length]

# NOTE these are placed outside the class scope purely to enable use of multiprocessing.Pool,
# which when called on a self function must pickle the self object
# and boto3 resource is not pickleable
s3_client = None
s3_directory = None
s3_iterator = iter([])

class ParquetBinPartitioner:
    '''
    This class loads parquet files from the given s3_directory, adds a column
    bin_id which is partitioned into bin_count bins
    '''

    def __init__(self, s3_client_in, s3_directory_in):
        global s3_iterator, s3_client, s3_directory
        s3_client = s3_client_in
        s3_directory = s3_directory_in
        s3_iterator = S3DirectoryIterator(s3_client, s3_directory, return_string=True)


    # def _recombine_partition_files(self, local_output_directory):
    #     # Loop over partition directories
    #     # Assumes 1-level partition only
    #     merge_partitioned_directory(local_output_directory)


    def _do_partition_file(self, s3_file_path:str, pos_interval_size:int, local_output_directory:str):

        logger.info('Loading parquet file ' + s3_file_path)
        df = pd.read_parquet(s3_file_path)

        # Run GC explicitly to free up previous dataframe if needed
        gc.collect()

        logger.info('Loaded %d rows to dataframe' % len(df))

        logger.info('Adding reference_bases and alternate_bases column')
        df['reference_bases'] = df['REF']
        df['alternate_bases'] = df['ALT']
        logger.info('Adding start_position and end_position column')
        df['start_position'] = df['POS'].astype('int')
        df['end_position'] = (df['start_position'] + df['reference_bases'].str.len()).astype('int')
        logger.info('Adding bin_id column')
        df['bin_id'] = (df['start_position'] / pos_interval_size).astype('int')

        logger.info('Dropping POS, REF, ALT columns')
        df.drop(columns=['POS', 'REF', 'ALT'], inplace=True)
        # df.drop('REF', axis=1, inplace=True)
        # df.drop('ALT', axis=1, inplace=True)

        logger.info('Finding unique bin_id values')
        # Use filtered dataframes in order to reduce memory footprint
        bin_id_list = df.bin_id.unique()
        bin_id_list = sorted(bin_id_list)
        logger.info('Writing bin_id files from %d to %d' % (
            bin_id_list[0], bin_id_list[-1]
        ))

        # collapse bin_id list to chunks of particular size
        chunked_bin_ids = chunk_list(bin_id_list, 100)
        partition_cols = ['bin_id']

        # Write df to partitioned parquet
        # df.to_parquet(path=local_output_directory, partition_cols=partition_cols)

        for bin_id_chunk in chunked_bin_ids:
            wheres = []
            for bin_id in bin_id_chunk:
                wheres.append('bin_id == %d' % bin_id)
            # query = ' or '.join(wheres)
            bin_df = df.query(' or '.join(wheres))
            logger.info('Writing parquet file for bin_id=%s' % str(bin_id_chunk))
            bin_df.to_parquet(path=local_output_directory, partition_cols=partition_cols)

            # Explicitly delete local df
            del bin_df

        # Explicitly delete object
        del df
        gc.collect()

    def partition(self, pos_interval_size:int, local_output_directory:str):
        max_count = math.inf
        count = 0
        pool_size = 10

        output_file_merge_interval = 4

        paths = []
        # global s3_iterator
        for s3_object_path in s3_iterator:
            if count >= max_count:
                logger.info('Breaking at loop %d' % max_count)
                break
            count += 1

            # s3_object_path = 's3://%s/%s' % (s3_object.bucket_name, s3_object.key)
            # self._do_partition_file(s3_object_path, pos_interval_size, local_output_directory)
            paths.append(s3_object_path)

        path_chunks = chunk_list(paths, pool_size)
        logger.info('Path chunks:\n%s' % path_chunks)
        count = 0
        for path_chunk in path_chunks:
            count += 1
            args = list(zip(
                path_chunk,
                [pos_interval_size]*len(path_chunk),
                [local_output_directory]*len(path_chunk)))

            logger.info('Calling pool.starmap with args=')
            for a in args:
                print(str(a))

            def err_callback(err):
                raise RuntimeError(err)

            # async_result = pool.starmap_async(self._do_partition_file, args, error_callback=err_callback)
            # async_result.wait()
            # future = pool.submit(self._do_partition_file, args)

            # NOTE highly recommended to use context manager for multiprocessing.Pool
            # there is potentially a memory leak not cleaned up by allowing it
            # to simply be garbage collected
            with Pool(processes=pool_size) as pool:
                pool.starmap(self._do_partition_file, args)
                # pool.shutdown(wait=True)

            logger.info('Finished pool execution')
            if count % output_file_merge_interval == 0:
                logger.info('Merging output directory')
                merge_partitioned_directory(local_output_directory, processes=12)


    # def partition(self, pos_interval_size, local_output_directory):
    #     # fs = s3fs.S3FileSystem()
    #     max_count = math.inf
    #     count = 0
    #     output_file_merge_interval = 10

    #     for s3_object in self.s3_iterator:
    #         if count >= max_count:
    #             logger.info('Breaking at loop %d' % max_count)
    #             break
    #         count += 1

    #         s3_object_path = 's3://%s/%s' % (s3_object.bucket_name, s3_object.key)
    #         logger.info('Loading parquet file ' + s3_object_path)
    #         df = pd.read_parquet(s3_object_path)

    #         # Run GC explicitly to free up previous dataframe if needed
    #         gc.collect()

    #         logger.info('Loaded %d rows to dataframe' % len(df))

    #         logger.info('Adding bin_id column')

    #         df['bin_id'] = (df['POS'].astype('int') / pos_interval_size).astype('int')

    #         logger.info('Finding unique bin_id values')
    #         # Use filtered dataframes in order to reduce memory footprint
    #         bin_id_list = df.bin_id.unique()
    #         bin_id_list = sorted(bin_id_list)
    #         logger.info('Writing bin_id files from %d to %d' % (
    #             bin_id_list[0], bin_id_list[-1]
    #         ))

    #         # collapse bin_id list to chunks of particular size
    #         chunked_bin_ids = chunk_list(bin_id_list, 100)
    #         partition_cols = ['bin_id']

    #         # Write df to partitioned parquet
    #         # df.to_parquet(path=local_output_directory, partition_cols=partition_cols)


    #         for bin_id_chunk in chunked_bin_ids:
    #             wheres = []
    #             for bin_id in bin_id_chunk:
    #                 wheres.append('bin_id == %d' % bin_id)
    #             query = ' or '.join(wheres)
    #             bin_df = df.query(query)
    #             logger.info('Writing parquet file for bin_id=%s' % str(bin_id_chunk))
    #             bin_df.to_parquet(path=local_output_directory, partition_cols=partition_cols)

    #             # Explicitly delete local df
    #             del bin_df

    #         # Explicitly delete object
    #         del df

    #         if count % output_file_merge_interval == 0:
    #             logger.info('Recombining partitioned files')
    #             self._recombine_partition_files(local_output_directory)

    #     # If not combined on the last run, combine now
    #     # if count % output_file_merge_interval != 0:
    #     #     logger.info('Done with loop, but outputs not combined on last run, combining now')
    #     #     self._recombine_partition_files(local_output_directory)
