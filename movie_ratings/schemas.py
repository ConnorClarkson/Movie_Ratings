from pyspark.sql.types import StructType, StructField, IntegerType, StringType

users_schema = StructType([
    StructField('user_id', IntegerType(), False),
    StructField('gender', StringType(), False),
    StructField('age', IntegerType(), False),
    StructField('occupation', IntegerType(), False),
    StructField('zip_code', StringType(), False)
])
movies_schema = StructType([
    StructField('movie_id', IntegerType(), False),
    StructField('title', StringType(), False),
    StructField('genres', StringType(), False)
])
ratings_schema = StructType([
    StructField('user_id', IntegerType(), False),
    StructField('movie_id', IntegerType(), False),
    StructField('rating', IntegerType(), False),
    StructField('timestamp', IntegerType(), False)
])
