import setuptools

setuptools.setup(
    name='dataset-partitioning',
    version='0.0.1',
    author='Kyle Ferriter',
    description='',
    url='https://github.com/theferrit32/dataset-partitioning',
    python_requires='>=3.6',
    packages=['dataset_partitioning'],
    install_requires=[
        'pandas==1.0.1',
        'pyarrow==14.0.1',
        'boto3==1.12.17',
        's3fs==0.4.0'
    ]
)
