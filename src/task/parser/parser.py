from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as f

"""
    The parser is the thing I am proud of the most :D
    It`s created to parse the complex nested JSON. 
    The parser has the schema that looks like this:
        (path.to.the.column, column name)
"""


class Parser:

    def __init__(self, schema: list):
        self.__schema = schema

    def parse(self, df: DataFrame) -> DataFrame:
        return df.select(f.explode('raw').alias('raw')) \
            .select(*list(map(self.__generate_paths, self.__schema)))

    @staticmethod
    def __generate_paths(schema: tuple[str, str]) -> Column:
        column_path, column_name = schema

        path_list = column_path.split('.')
        col = f.col('raw')
        for field in path_list:
            col = col.getItem(field)

        return col.alias(column_name)


def parse_df(df: DataFrame, *schema: list[tuple[str, str]]) -> DataFrame:
    return Parser(*schema).parse(df)
