from unittest import TestCase
from unittest.mock import MagicMock

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from movie_ratings.process_data import calculate_movies_data, calculate_top_three


class Test(TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a local SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("LogicTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after all tests are finished
        cls.spark.stop()

    def test_calculate_movies_data(self):
        movies_data = [('1', 'movie_1'),
                       ('2', 'movie_2'),
                       ('3', 'movie_3'),
                       ]
        ratings_data = [('1', 5),
                        ('1', 1),
                        ('2', 3),
                        ('2', 3),
                        ('3', 2),
                        ('3', 3),
                        ]
        movies = self.spark.createDataFrame(movies_data, schema='movie_id string, title string')
        ratings = self.spark.createDataFrame(ratings_data, schema='movie_id string, rating long')
        results_df = calculate_movies_data(movies, ratings)

        self.assertIn("max_rating", results_df.columns, "max_rating should be in returned df.")
        self.assertIn("min_rating", results_df.columns, "min_rating should be in returned df.")
        self.assertIn("avg_rating", results_df.columns, "avg_rating should be in returned df.")
        self.assertEqual(5, results_df.take(1)[0]['max_rating'], "max_rating should be equal to 5.")
        self.assertEqual(1, results_df.take(1)[0]['min_rating'], "max_rating should be equal to 1.")
        self.assertEqual(3.0, results_df.take(1)[0]['avg_rating'], "max_rating should be equal to 3.0.")
        self.assertEqual(3, results_df.take(3)[2]['max_rating'], "max_rating should be equal to 3.")
        self.assertEqual(2, results_df.take(3)[2]['min_rating'], "max_rating should be equal to 2.")
        self.assertEqual(2.5, results_df.take(3)[2]['avg_rating'], "max_rating should be equal to 2.5.")

    def test_calculate_movies_data_exception(self):
        mock_movies = MagicMock()
        mock_movies.join.side_effect = AnalysisException('Test Error')
        mock_ratings = MagicMock()
        with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
            calculate_movies_data(mock_movies, mock_ratings)
        self.assertIn("ERROR:root:Error calculating new table.", cm.output[0])

    def test_calculate_top_three(self):
        movies_data = [
            ('1', 'Test Movie'),
            ('2', 'Test Movie 2'),
            ('3', 'Test Movie 3'),
            ('4', 'Test Movie 4'),
            ('5', 'Test Movie 5'),
        ]
        ratings_data = [
            ('1', '1', 5, 978301368),
            ('1', '2', 2, 978301378),
            ('1', '3', 4, 978301388),
            ('1', '4', 1, 978301398),
            ('1', '5', 4, 978301468),
                        ]
        movies = self.spark.createDataFrame(movies_data, schema='movie_id string, title string')
        ratings = self.spark.createDataFrame(ratings_data,
                                             schema='user_id string, movie_id string, rating long, timestamp long')
        results_df = calculate_top_three(movies, ratings)
        self.assertIn('user_id', results_df.columns)
        self.assertIn('title', results_df.columns)
        self.assertIn('rating', results_df.columns)
        self.assertIn('rank', results_df.columns)
        self.assertEqual(3, results_df.count())
        self.assertEqual("Test Movie", results_df.take(3)[0]['title'])
        self.assertEqual(5, results_df.take(3)[0]['rating'])
        self.assertEqual("Test Movie 3", results_df.take(3)[1]['title'])
        self.assertEqual(4, results_df.take(3)[1]['rating'])
        self.assertEqual("Test Movie 5", results_df.take(3)[2]['title'])
        self.assertEqual(4, results_df.take(3)[2]['rating'])


    def test_calculate_top_three_exception(self):
        mock_movies = MagicMock()
        mock_ratings = MagicMock()
        mock_ratings.withColumn.side_effect = AnalysisException('Test Error')
        with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
            results = calculate_top_three(mock_movies, mock_ratings)
        self.assertIn("ERROR:root:Error calculating new table.", cm.output[0])
