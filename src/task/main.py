from pyspark.sql import SparkSession
from task.processor.processor import Processor

"""
    Hi! That is my Scytale home assignment.
    Here I tried to made this task as close to
    production as possible in this period.
    So, that`s the starting point of this program.
    Here I initialized the SparkSession and 
    the kwargs for the processor.
"""


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('Scytale Home Assignment')\
        .getOrCreate()

    kwargs = {
              'repos_path': 'resources/repos.json',
              'prs_paths': ['resources/prs-e2e.json', 'resources/prs-dart.json'],
              'output_path': 'resources/output/'
             }

    task = Processor(spark).run(**kwargs)
