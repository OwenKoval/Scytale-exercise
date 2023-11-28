"""
    Utils file with main functions
"""

from pyspark.sql import SparkSession


def spark_builder(appname='Test', partitions=4):
    """
    The entry point of SparkSession
    :param appname: set the name of your SparkSession
    :param partitions: set the number of partitions
    """
    return (SparkSession.builder
            .master('local')
            .appName(appname)
            .config('spark.shuffle.partitions', partitions)
            .getOrCreate())


def read_json(spark, path=None, schema_json=None,
              header='false', multiline=True):
    """
    :param spark: SparkSession to read the file
    :param path: path to file to read it
    :param schema_json: use schema which defined by developer
    :param header: true or false use header in reading df
    :param multiline: true or false use multiline in reading df
    :return df: dataframe with data
    """
    if path is None:
        raise ValueError('Path cannot be None')

    return (spark.read
            .options(header=header, multiline=multiline)
            .json(path, schema=schema_json))


def write_to_parquet(df, file_path='', header='true'):
    """
    Write the df to folder
    :param df: result dataframe after transformation
    :param file_path: path to folder where result will save
    :param header: true or false use header in writing result
    """
    (df.write
     .option('header', header)
     .mode('overwrite')
     .parquet(file_path))

