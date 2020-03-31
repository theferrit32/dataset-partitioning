import os, sys, gc
import pandas as pd
import logging
import binascii

from multiprocessing import Pool

# logging.basicConfig(level=logging.DEBUG)
from dataset_partitioning import logging_util
logger = logging_util.get_logger(__name__)

def rand_string(length):
    return binascii.hexlify(os.urandom(int(length/2 + 1))).decode('utf-8')[:length]

def merge_parquet_directory(directory_path):
    if os.path.isdir(directory_path) is False:
        msg = 'Directory does not exist: ' + directory_path
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info('Merging parquet files in directory: %s' % directory_path)
    df = None
    parquet_filenames = [f for f in os.listdir(directory_path) if f.endswith('.parquet')]
    parquet_paths = ['%s/%s' % (directory_path, f) for f in parquet_filenames]
    total_records_initial = 0
    for fpath in parquet_paths:
        if df is None:
            df = pd.read_parquet(fpath) # use_pandas_metadata=False
            total_records_initial += len(df)
        else:
            df_new = pd.read_parquet(fpath)
            total_records_initial += len(df_new)
            df = df.append(df_new)
            del df_new
        gc.collect()
    logger.info('Finished reading parquet files %s' % (str(parquet_paths)))
    logger.debug('Read %d initial records' % (total_records_initial))
    combined_file_path = '%s/%s' % (
        directory_path, rand_string(16) + '.parquet'
    )
    logger.info('Sorting by POS')
    # TODO sorting may help, pandas to_parquet may already group values though
    # df.sort_values('POS', axis=0)
    logger.info('Writing files %s to combined file %s' % (str(parquet_paths), combined_file_path))
    df.to_parquet(path=combined_file_path, index=False)
    for fpath in parquet_paths:
        os.remove(fpath)
    logger.info('Deleted old files')
    return combined_file_path


def merge_partitioned_directory(partitioned_path, processes=1):

    if os.path.isdir(partitioned_path) is False:
        raise RuntimeError('Directory does not exist: %s' % partitioned_path)

    paths = []
    for parquet_directory in os.listdir(partitioned_path):
        path = os.path.join(partitioned_path, parquet_directory)
        paths.append((path,))

        # combined_file = merge_parquet_directory(path)
        # logger.info('Created file %s from directory %s' % (combined_file, path))

    with Pool(processes=processes) as pool:
        pool.starmap(merge_parquet_directory, paths)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise RuntimeError('Must provide dir name')

    merge_partitioned_directory(sys.argv[1])

