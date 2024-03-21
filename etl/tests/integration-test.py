import pytest
from etl.main import main
from pyspark.sql import SparkSession

def test_etl_pipeline():
    # Define test parameters
    test_source = "./data/test_datasets/test.csv"
    test_database = "warehouse"
    test_table = "test_customers"
    spark_env = "local"
    db_env = "postgres"
    jdbc_driver_jar = "./postgresql-42.7.3.jar"
    jdbc_url = f"jdbc:postgresql://{db_env}:5432/{test_database}"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("IntegrationTestApp") \
        .master(spark_env) \
        .config("spark.driver.extraClassPath", jdbc_driver_jar) \
        .config("spark.executor.extraClassPath", jdbc_driver_jar) \
        .getOrCreate()

    # Run the ETL pipeline
    main(test_source, test_database, test_table)

    # Read the result back from the database
    result_df = spark.read.jdbc(url=jdbc_url, table=test_table, properties=connection_properties)

    # check all columns are present
    expected_columns = ["custId", "favourite_product", "longest_streak"]
    assert all(col_name in result_df.columns for col_name in expected_columns), "Missing columns in the result"

    # checking datatypes for each columns
    expected_datatypes = ["string", "string", "bigint"]
    for col_name, expected_dtype in zip(result_df.columns, expected_datatypes):
        actual_dtype = next(dtype for name, dtype in result_df.dtypes if name == col_name)
        assert actual_dtype.lower() == expected_dtype.lower(), f"Column '{col_name}' has unexpected data type: expected {expected_dtype}, got {actual_dtype}"

    # check data has been loaded to warehouse
    assert result_df.count() > 0, "Result DataFrame is empty"


    spark.stop()
