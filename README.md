# Sertis Data Engineering Take-Home Test

## Overview

We have a system that collects transactional data and stores them as CSV files. A sample input can be found in `data/transaction.csv`. Your task is to build an ETL pipeline using Python and Apache Spark, which aggregates the transactions by user ID and stores the results in another location in Parquet format.

Before you get started, a couple of tips:

* Feel free to structure your application as you see fit and add modules and packages if necessary.
* Make sure to provide documentation and comments for your solution as needed
* Imagine you are writing production-level code here. Other than functionality, we also pay close attention to areas such as code architecture, code style, readability, maintainability, etc.
* We do consider incomplete submissions. If you struggle to complete the assignment, you may take shortcuts by changing requirements and let us know what you've changed

## Task 1. ETL Pipeline Implementation

### Preparation
Please make sure you are using Python `3.10` or above and Spark `3.3.3` or above in this task. You could run Spark local mode or use our provided `docker-compose` file to run Spark standalone mode on your machine.
Hint: Either Java 8 / 11 / 17 is required to run Spark.

### Requirements

* `main.py` should be the entry point to your ETL tool. We would like to ask you to build a Python command line tool that accepts the following arguments:

  | Argument    | Description                                                                |
  |-------------|----------------------------------------------------------------------------|
  | source      | Path to the source `csv` file                                              |
  | destination | Path to the output `parquet` file, which will contain the transformed data |

  For example, we should be able to run the application by executing the following command:
  
  ```shell
  python main.py \
    --source /opt/data/transaction.csv \
    --destination /opt/data/out.parquet
  ```

* The output file should adhere to the following schema:

  | Column      | Description |
  | ----------- | ----------- |
  | customer_id      | Column `custId` from source data       |
  | favourite_product   | Column `productSold` from source data which has greatest total units sold for this customer |
  | longest_streak   | The longest period of consecutive days when this customer made purchases. For example, if the customer bought items on 2011-08-10, 2011-08-11, 2011-08-14, 2011-08-15 and 2011-08-16, value of `longest_streak` would be 3.  |
  
* File `main.py` contains snippets on how to get started. Feel free to use either Spark DSL (PySpark) or SQL syntax for transformations.

* Add some unit tests and an integration test to test the functionality of your ETL pipeline.


### Bonus

If you're up for an extra challenge and want to show us some more of your skills, consider doing the following:

* Use Poetry as a package manager
* We have included a `docker-compose` file to run a standalone spark cluster locally. Dockerize your application into a `Dockerfile` and add a `docker-compose` service for it. It is recommended to use the [official Python image](https://hub.docker.com/_/python) as a base.
* After transforming the data, instead of dumping the transformed data onto the file system, insert it into a PostgreSQL database. To do this, add a docker container for PostgreSQL 13 to the `docker-compose` file, and insert the transformed data into a target database named `warehouse`, with a target table named `customers`, following the same schema as the output.

If you complete these additional requirements, you should be able to run the pipeline using (assuming the application is called `etl` in `docker-compose`):

```shell
docker compose build
docker compose run etl python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse \
  --table customers
```

After this we should be able to query the aggregated data from PostgreSQL (assuming the PostgreSQL instance is called `db` in `docker-compose`):

```shell
docker compose exec db psql --user postgres -d warehouse \
  -c 'select * from customers limit 10'
```

### Submission
Implement the requirements in this Git repository and create a [patch file](https://git-scm.com/docs/git-format-patch) for the changes. Include the patch file with your submitted documents.

## Task 2. System Architecture

### Preparation

This task does not have to be implemented. Imagine you write a proposal to us for data warehouse solution on Cloud. 

Choose one of the major cloud platforms, *Amazon Web Services*, *Google Cloud Platform* or *Microsoft Azure* as a basis of your solution and use services provided by the chosen platform. 

### Requirements

Describe how you would implement the following system in the cloud platform of your choice:

* The source data is stored in an on-premise RDBMS. The schema is like the transactional data in Part 1.
* A daily ETL pipeline needs to ingest the data, apply the same transformations as in Part 1 and store the output in the cloud where it can be queried.
* Ideally, the solution should be scalable in terms of data amount.

Feel free to make your own assumptions (e.g. use any services on the chosen cloud platform) and be as creative as you want with your answer. Remember to cover the requirements i.e. how the data is moved to the cloud, how the aggregations are calculated in your proposed solution and how scalability is achieved.

### Bonus
* Propose solutions covering the following areas:
  * Monitoring
  * Logging
  * Automated notifications
  * Cost-effective
  * Data Governance  (You could explain your understanding on Data Governance)
* Propose solutions on an additional cloud platform, covering the same requirements

### Submission
Explanation of the proposed solution, chosen tools/frameworks, justification on why you chose them, and any other supporting documentation, in PDF or markdown format.

