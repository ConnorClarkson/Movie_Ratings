import logging

from pyspark.errors import AnalysisException
from pyspark.sql import Window, dataframe
from pyspark.sql.functions import avg, max, min, round, row_number, desc, col


def calculate_movies_data(movies: dataframe, ratings: dataframe) -> dataframe:
    """
    Business logic to create a table with min, max, and average ratings for movies
    :param movies: Movies Dataframe
    :param ratings: Ratings Dataframe
    :return: PySpark dataframe with max_rating, min_rating, and avg_rating columns added
    """
    try:
        joined_movies_and_ratings = movies.join(ratings, 'movie_id')

        movie_ratings_data = joined_movies_and_ratings.groupBy('movie_id', 'title') \
            .agg(max('rating').alias('max_rating'),
                 min('rating').alias('min_rating'),
                 round(avg('rating'), 2).alias('avg_rating')) \
            .orderBy('movie_id')

        return movie_ratings_data
    except AnalysisException as ae:
        logging.error(f"Error calculating new table.\n{ae}")
        exit(1)


def calculate_top_three(movies: dataframe, ratings: dataframe) -> dataframe:
    """
    Business logic to create a table which has the users top 3 movie rankings.
    :param ratings: Ratings Dataframe
    :param movies: Movies Dataframe
    :return: PySpark dataframe with user_id, title, rating, rank, and timestamp
    """

    try:
        # define a window  that partitions by user_id and orders by rating in descending order
        user_window = Window.partitionBy("user_id").orderBy(desc("rating"))

        # add a row number column to each row based on the user_window
        ratings_ranked = ratings.withColumn("rank", row_number().over(user_window))

        # filter the rows with rank <= 3 to get each user's top 3 movies
        user_top3_movies = ratings_ranked.filter("rank <= 3")

        user_top3_movies_joined = user_top3_movies.join(movies, 'movie_id')
        selected_columns = [
            col('user_id'),
            col('title'),
            col('rating'),
            col('rank'),
        ]

        user_top3_results = user_top3_movies_joined.select(selected_columns)

        return user_top3_results
    except AnalysisException as ae:
        logging.error(f"Error calculating new table.\n{ae}")
        exit(1)
