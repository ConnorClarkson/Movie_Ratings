import logging
import sys

from movie_ratings.file_io import read_file, output_file
from movie_ratings.process_data import calculate_movies_data, calculate_top_three
from movie_ratings.schemas import movies_schema, ratings_schema
from movie_ratings.utils import parse_args
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime


if __name__ == '__main__':
    try:
        # parse command line args
        args = parse_args(sys.argv[1:])
    except Exception as e:
        logging.error(f"Unable to parse args.\n {e}")
        exit(1)

    try:
        # Makefile spark session
        spark = SparkSession.builder.appName("Movie Ratings").getOrCreate()
    except Exception as e:
        logging.error(f"Unable to create SparkSession\n {e}")
        exit(1)
    # read data
    try:
        logging.info("Reading data.")
        movies = read_file(spark, args.movies, movies_schema)
        ratings = read_file(spark, args.ratings, ratings_schema)
    except Exception as e:
        logging.error(f"Uncaught Exception while reading inputs.\n {e}")
        exit(1)

    try:
        # Convert epoch seconds to DateTime
        ratings = ratings.withColumn("timestamp", from_unixtime(ratings.timestamp / 1000))
        logging.info("Calculating tables.")
        movie_ratings_results = calculate_movies_data(movies, ratings)
        user_top3_results = calculate_top_three(movies, ratings)
    except Exception as e:
        logging.error(f"Uncaught Exception while generating new tables.\n {e}")
        exit(1)

    try:
        # output to parquet
        logging.info("Outputting dataframes.")
        output_file(movie_ratings_results, f'{args.output}/movie_ratings')
        output_file(user_top3_results, f'{args.output}/top_three')
    except Exception as e:
        logging.error(f"Uncaught Exception while outputting files.\n {e}")
