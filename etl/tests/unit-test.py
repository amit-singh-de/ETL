import pytest
from etl.main import favoriteProduct, longestStreak, generateFinalResult
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import datetime

# Define a fixture to create a SparkSession
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("UnitTest") \
        .master("local") \
        .getOrCreate()
    yield spark
    spark.stop()

# Define test functions
def test_favoriteProduct(spark_session):
    # Define schema for test DataFrame
    schema = StructType([
        StructField("transactionId", StringType(), True),
        StructField("custId", StringType(), True),
        StructField("transactionDate", DateType(), True),
        StructField("productSold", StringType(), True),
        StructField("unitsSold", IntegerType(), True)
    ])

    # Define test data
    test_data = [
        ('1', 'A', datetime.strptime('2024-01-01', '%Y-%m-%d'), 'X', 10),
        ('2', 'A', datetime.strptime('2024-01-02', '%Y-%m-%d'), 'Y', 20),
        ('2', 'A', datetime.strptime('2024-01-03', '%Y-%m-%d'), 'Y', 20),
        ('3', 'B', datetime.strptime('2024-01-01', '%Y-%m-%d'), 'X', 30),
        ('4', 'B', datetime.strptime('2024-01-02', '%Y-%m-%d'), 'Y', 40)
    ]

    # Create Spark DataFrame
    test_df = spark_session.createDataFrame(test_data, schema)

    # Call the function
    result_df = favoriteProduct(test_df)

    # Add assertions to check the correctness of the result
    assert result_df.select("custId", "favourite_product").collect() == [("A", "Y"), ("B", "Y")]

def test_longestStreak(spark_session):
    # Define schema for test DataFrame
    schema = StructType([
        StructField("transactionId", StringType(), True),
        StructField("custId", StringType(), True),
        StructField("transactionDate", DateType(), True),
        StructField("productSold", StringType(), True),
        StructField("unitsSold", IntegerType(), True)
    ])

    # Define test data
    test_data = [
        ('1', 'A', datetime.strptime('2024-01-01', '%Y-%m-%d'), 'X', 10),
        ('2', 'A', datetime.strptime('2024-01-02', '%Y-%m-%d'), 'Y', 20),
        ('3', 'B', datetime.strptime('2024-01-01', '%Y-%m-%d'), 'X', 30),
        ('4', 'B', datetime.strptime('2024-01-02', '%Y-%m-%d'), 'Y', 40)
    ]

    # Create Spark DataFrame
    test_df = spark_session.createDataFrame(test_data, schema)

    # Call the function
    result_df = longestStreak(test_df)

    # Add assertions to check the correctness of the result
    assert result_df.select("custId", "longest_streak").collect() == [("A", 2), ("B", 2)]

def test_generateFinalResult(spark_session):
    # Define schema for test DataFrame
    schema = StructType([
        StructField("transactionId", StringType(), True),
        StructField("custId", StringType(), True),
        StructField("transactionDate", DateType(), True),
        StructField("productSold", StringType(), True),
        StructField("unitsSold", IntegerType(), True)
    ])

    # Define test data for favoriteProduct function
    favorite_product_data = [
        ('A','Y'),
        ('B','Y')
    ]

    # Create Spark DataFrame for favoriteProduct function
    favorite_product_df = spark_session.createDataFrame(favorite_product_data, ['custId','favourite_product'])

    # Create Spark DataFrame for longestStreak function
    longest_streak_data = [
        ('A',2),
        ('B',2)
    ]
    longest_streak_df = spark_session.createDataFrame(longest_streak_data, ['custId', 'longest_streak'])

    # Define expected result
    expected_result_data = [
        ('A', 'Y', 2),
        ('B', 'Y', 2)
    ]

    # Create expected result Spark DataFrame
    expected_result_df = spark_session.createDataFrame(expected_result_data, ['custId', 'favourite_product', 'longest_streak'])

    # Call the function
    result_df = generateFinalResult(favorite_product_df, longest_streak_df)

    # Add assertions to check the correctness of the result
    assert result_df.collect() == expected_result_df.collect()
