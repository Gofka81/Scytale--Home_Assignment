from pyspark.sql import SparkSession, DataFrame
from task.reader.reader import Reader
from task.parser.parser import parse_df
import pyspark.sql.functions as f

"""
    Processor is the core class in this program.
    I tried to stick to ETL process during the coding,
    just to emulate the real pipeline job ^^ 
    Added here kwargs as well to remove all hardcoded 
    paths outside the program.
"""


class Processor:

    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def run(self, **kwargs):
        raw_repos, raw_prs = self.__extract(kwargs['repos_path'], kwargs['prs_paths'])
        final_df = self.__transform(raw_repos, raw_prs)
        self.__load(final_df, kwargs['output_path'])

    def __extract(self, repos_path: str, prs_path: list) -> tuple[DataFrame, DataFrame]:
        reader = Reader(self.__spark)
        repos_df = reader.read_repos(repos_path)
        prs_df = reader.read_prs(prs_path)

        return repos_df, prs_df

    @staticmethod
    def __transform(raw_repos, raw_prs) -> DataFrame:
        repos_schema = [('id', 'repository_id'), ('name', 'repository_name'), ('owner.login', 'repository_owner')]
        prs_schema = [('id', 'pr_id'), ('state', 'pr_state'), ('merged_at', 'pr_merged_date'),
                      ('head.repo.id', 'repository_id')]

        repos_df = parse_df(raw_repos, repos_schema)
        prs_df = parse_df(raw_prs, prs_schema)

        return repos_df.join(prs_df, ['repository_id'], 'left') \
            .groupBy('repository_id', 'repository_name', 'repository_owner') \
            .agg(f.count('pr_id').alias('num_prs'),
                 f.count(f.when((f.col('pr_state') == 'closed'), True)).alias('num_prs_closed'),
                 f.max('pr_merged_date').alias("merged_at")) \
            .withColumn('is_compliant', (f.col('num_prs') == f.col('num_prs_closed'))
                        & f.col('repository_owner').contains("scytale"))

    @staticmethod
    def __load(df: DataFrame, output_path: str):
        df.write.json(output_path)
        df.show()   # Just to show the output in the terminal
