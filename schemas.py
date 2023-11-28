"""
    The file which set the schema for dataframe
"""

import pyspark.sql.types as t

github_prs_schema = t.StructType([
    t.StructField('state', t.StringType(), True),
    t.StructField('merged_at', t.TimestampType(), True),
    t.StructField('head', t.StructType([
        t.StructField('repo', t.StructType([
            t.StructField('id', t.IntegerType(), True),
            t.StructField('name', t.StringType(), True),
            t.StructField('full_name', t.StringType(), True),
            t.StructField('owner', t.StructType([
                t.StructField('login', t.StringType(), True)
            ]), True),
        ]), True),
    ]), True),
])
