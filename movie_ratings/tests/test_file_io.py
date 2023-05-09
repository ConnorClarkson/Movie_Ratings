import contextlib
import os
from io import StringIO
from unittest import TestCase
from unittest.mock import Mock, MagicMock
from tempfile import TemporaryDirectory, TemporaryFile

from pyspark.sql import SparkSession

from movie_ratings.file_io import read_file, output_file
from movie_ratings.schemas import users_schema


class TestFileIO(TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a local SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("IOTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after all tests are finished
        cls.spark.stop()

    def test_read_file(self):
        """Test Sunny day path of the method being able to read a .dat from disk."""
        data = read_file(self.spark, f'{os.path.dirname(__file__)}/fixtures/test_user.dat', users_schema)
        self.assertTrue(data, "File should of been read and returned.")
        self.assertEqual(data.count(), 5, "Row count should equal 5.")

    def test_read_file_not_exist(self):
        """Test the method correctly exits if file path doesn't exist"""
        with TemporaryDirectory() as td:
            with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
                read_file(None, f"{td}/non_exists/file.csv", None)
            self.assertIn(f"ERROR:root:File does not exist.\n{td}/non_exists/file.csv", cm.output[0])

    def test_read_file_io_error(self):
        """Test the method correctly exits if the read throws a IO Error"""
        with TemporaryDirectory() as td:
            os.mkdir(f"{td}/exists")
            with open(f"{td}/exists/file.csv", 'w') as f:
                f.write("Test File")

            with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
                mock_spark = MagicMock()
                mock_spark.read.csv.side_effect = IOError
                read_file(mock_spark, f"{td}/exists/file.csv", None)
            self.assertIn(f"ERROR:root:Error reading file.\n", cm.output[0])

    def test_output_file(self):
        with TemporaryDirectory() as td:
            data = [("col1", "col2", "col3"),
                    ("a", "b", "c"),
                    ("1", "2", "3")]
            df = self.spark.createDataFrame(data)
            output_file(df, f"{td}/test_data")
            self.assertTrue(os.path.exists(f"{td}/test_data"))
            files = os.listdir(f"{td}/test_data")
            self.assertIn("_SUCCESS", files, "Output should have a _SUCCESS file")
            self.assertTrue(len([x for x in files if x.endswith('.parquet')]) == 4,
                            "Output should of produced 4 parquet files.")

            read_df = self.spark.read.parquet(f"{td}/test_data")
            self.assertEqual(sorted(df.collect()), sorted(read_df.collect()),
                             "Original df and df from parquet should be the same.")

    def test_output_file_io_exception(self):
        """Test the method correctly exits if writing parquet throws an IOError"""

        with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
            mock_dataframe = MagicMock()
            mock_dataframe.write.parquet.side_effect = IOError
            output_file(mock_dataframe, None)
        self.assertIn("ERROR:root:Unable to write parquet due to an IOError.", cm.output[0])

    def test_output_file_exception(self):
        """Test the method correctly exits if writing parquet throws an Exception"""
        with self.assertRaises(SystemExit) as se, self.assertLogs('root', 'ERROR') as cm:
            mock_dataframe = MagicMock()
            mock_dataframe.write.parquet.side_effect = Exception
            output_file(mock_dataframe, None)
        self.assertIn("ERROR:root:Unable to write parquet.\n", cm.output[0])
