from functools import reduce
from pyspark.sql import DataFrame, SparkSession

"""
    The reader class is created as an util
    to generalize the reading process.
"""


class Reader:

    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def read_repos(self, path: str) -> DataFrame:
        return self.__read(path)

    def read_prs(self, paths: list[str]) -> DataFrame:
        def union(right: DataFrame, left: DataFrame) -> DataFrame:
            return right.union(left)

        pr_df_list = list(map(self.__read, paths))
        return reduce(union, pr_df_list)

    def __read(self, path: str) -> DataFrame:
        return self.__spark.read.option('multiline', True).json(path)

