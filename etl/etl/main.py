import argparse
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import DateType,StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import sum,col,row_number,desc,date_add,dense_rank,count,max
from pyspark.sql.window import Window


def main(source:str,database:str,table:str):

    """
    Main function to perform ETL process and store results in PostgreSQL table.

    Args:
        source_path (str): Path to the input CSV file.
        destination_database (str): Name of the destination database in PostgreSQL.
        destination_table (str): Name of the destination table in PostgreSQL.
    """

    # Path to the PostgreSQL JDBC driver JAR
    jdbc_driver_jar = "./postgresql-42.7.3.jar"

    # Initialize SparkSession
    spark = SparkSession.builder.master('local') \
                    .appName("etl-prod") \
                    .config("spark.driver.extraClassPath", jdbc_driver_jar) \
                    .config("spark.executor.extraClassPath", jdbc_driver_jar) \
                    .getOrCreate()

    # print source and destination, can be used with loggin.info for log analytics and Data Governance
    print(f"source -> {source}")
    print(f"destination -> {database}.{table}")


    # Define schema for reading the dataset
    schema = StructType([
        StructField("transactionId", StringType(), True),
        StructField("custId", StringType(), True),
        StructField("transactionDate", DateType(), True),
        StructField("productSold", StringType(), True),
        StructField("unitsSold", IntegerType(), True)
    ])

    # Read the dataset
    df = spark.read.csv(source, sep='|', header=True,schema=schema)

    # Perform Transformations
    favorite_products_df = favoriteProduct(df)
    longest_streak_df = longestStreak(df)
    final_result_df = generateFinalResult(favorite_products_df, longest_streak_df)

    # Define the PostgreSQL connection properties
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Construct URL for the destination database
    jdbc_url=f"jdbc:postgresql://postgres:5432/{database}"

    # Write to PostgreSQL table
    final_result_df.write.jdbc(url=jdbc_url, table=table, mode="overwrite", properties=connection_properties)


def favoriteProduct(df:DataFrame) -> DataFrame:
    """
    Identify the favorite product for each customer based on total units sold.

    Args:
        df (DataFrame): Input DataFrame containing transaction data.

    Returns:
        DataFrame: DataFrame with columns 'custId' and 'favorite_product'.
    """

    # Find the total units of products sold per user
    total_units_sold_df = df.groupBy('custId','productSold').agg(sum('unitsSold').alias("total_units_sold"))
    # Rank the products for each user based on the total units sold per user
    window_spec = Window.partitionBy("custId").orderBy(col("total_units_sold").desc())
    favorite_product_df = total_units_sold_df.withColumn("rank", row_number().over(window_spec))
    favorite_product_df = favorite_product_df.filter(col("rank") == 1).drop("rank","total_units_sold").withColumnRenamed('productSold','favourite_product')

    return favorite_product_df



def longestStreak(df:DataFrame)->DataFrame:

    """
    Identify the longest streak of consecutive days of transactions for each customer.

    Args:
        df (DataFrame): Input DataFrame containing transaction data.

    Returns:
        DataFrame: DataFrame with columns 'custId' and 'longest_streak'.
    """

    # Find distinct dates for each user to calculate consecutive records based on dates
    distinct_date_df = df['custId','transactionDate'].dropDuplicates(["custId", "transactionDate"])

    # Rank each date for customers
    rank_windowSpec = Window.partitionBy('custId').orderBy('transactionDate')
    ranked_df = distinct_date_df.withColumn("rank",dense_rank().over(rank_windowSpec))

    # Create a date group for each record for each user
    consecutive_df = ranked_df.withColumn("date_group",date_add(col("transactiondate"),-col("rank")))

    # Group based on customer_id and date_group and find the number of consecutive days
    # All records for a customer that have the same date_group signify consecutive dates
    consecutivedays_df=consecutive_df.groupBy('custId','date_group').agg(count('date_group').alias('consecutiveDays'))

    # Aggregate the records based on customer_id to find the max consecutive days for each customer_id
    result = consecutivedays_df.groupBy('custId').agg(max('consecutiveDays').alias('longest_streak'))

    return result


"""
generateFinalResult function take the output of favoriteProduct
and longestStreak and join both on custId to produce the final expected result
"""

def generateFinalResult(favoriteProduct:DataFrame,longestStreak:DataFrame) -> DataFrame :
    """
    Generate the final result by joining favorite products and longest streaks.

    Args:
        favorite_product_df (DataFrame): DataFrame containing favorite product per customer.
        longest_streak_df (DataFrame): DataFrame containing longest streak per customer.

    Returns:
        DataFrame: Final DataFrame with columns from both input DataFrames.
    """

    finalResult_df = favoriteProduct.join(longestStreak,on=('custId'))

    return finalResult_df



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process input and output file paths.')
    parser.add_argument('--source', type=str, help='Path to the input CSV file')
    parser.add_argument('--database', type=str, help='name of destination database in postgres')
    parser.add_argument('--table', type=str, help='name of destination table in postgres')
    args = parser.parse_args()
    main(args.source, args.database,args.table)

