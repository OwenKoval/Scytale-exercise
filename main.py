"""
    File with the Solution
"""
import os
import logging
import json
import requests

import pyspark.sql.functions as f

from schemas import github_prs_schema
from const import GITHUB_ORGANIZATION, GITHUB_PRS_PATH, INPUT_DATA_FOLDER
from utils import spark_builder, read_json, write_to_parquet

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def get_github_data(organization):
    """
        Write all repositories pull requests as json files in folder
    :param organization: constant to access the needed repository
    """
    repositories_url = f'https://api.github.com/orgs/{organization}/repos'
    repositories_response = requests.get(repositories_url, timeout=30).json()

    os.makedirs(f'{INPUT_DATA_FOLDER}/{organization}', exist_ok=True)

    for repo in repositories_response:
        repo_name = repo['name']
        repo_url = f'https://api.github.com/repos/{organization}/{repo_name}/pulls'

        pulls_response = requests.get(repo_url, timeout=30).json()

        file_path = f'{INPUT_DATA_FOLDER}/{organization}/{repo_name}_pulls.json'

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(pulls_response, file, indent=2)

        logging.info(f'Pull requests for {repo_name} saved to {file_path}')


def transform_data(spark, github_organization, schema):
    """
        Find the numbers of prs and numbers with closed prs per repository ID and date of last merged PR
    :param spark: SparkSession
    :param github_organization: constant to access the needed repository
    :param schema: use schema which defined by developer
    :return: df with numbers of prs and numbers with closed prs per repository
    """

    raw_df = read_json(spark=spark,
                       path=f'{INPUT_DATA_FOLDER}/{github_organization}/*.json',
                       schema_json=schema)

    total_count = f.col('state').isNotNull()
    closed_count = f.col('state') == 'closed'

    cleaned_df = (raw_df
                  .select('state', 'merged_at', f.col('head.repo.id').alias('repository_id'),
                          f.col('head.repo.name').alias('repository_name'),
                          f.col('head.repo.owner.login').alias('repository_owner'),
                          f.substring_index('head.repo.full_name', '/', 1).alias('organization_name'))
                  .cache())

    result_df = (cleaned_df
                 .groupBy('repository_id', 'repository_name', 'repository_owner', 'organization_name')
                 .agg(f.count(f.when(total_count, 1)).alias('num_prs'),
                      f.count(f.when(closed_count, 1)).alias('num_closed_prs'),
                      f.max('merged_at').alias('last_merged_date'))
                 .withColumn('is_compliant',
                             f.when((f.col('num_prs') == f.col('num_closed_prs'))
                                    & (f.lower(f.col('repository_owner')).contains('scytale')), True)
                             .otherwise(False)))

    return result_df


if __name__ == '__main__':
    spark = spark_builder(appname='ReadJsonFiles')

    get_github_data(GITHUB_ORGANIZATION)

    df = transform_data(spark=spark,
                        github_organization=GITHUB_ORGANIZATION,
                        schema=github_prs_schema)

    write_to_parquet(df=df,
                     file_path=GITHUB_PRS_PATH)
