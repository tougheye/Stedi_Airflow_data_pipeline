# Data Pipelines with Airflow

This project is built on the solid understanding of Apache Airflow's core concepts that creates custom operators to execute essential functions like staging data, populating a data warehouse, and validating data through the pipeline.

The project template streamlines imports and includes four unimplemented operators. These operators need to be turned into functional components of a data pipeline while being interconnected for a coherent and logical data flow.

A helper class containing all necessary SQL transformations is at your disposal to execute them using your custom operators.

## Initiating the Airflow Web Server
[Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.

To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

```bash
docker-compose up -d
```
Visit http://localhost:8080 once all containers are up and running.

## Configuring Connections in the Airflow Web Server UI
![Airflow Web Server UI. Credentials: `airflow`/`airflow`.](assets/login.png)

On the Airflow web server UI, use `airflow` for both username and password.

## Getting Started with the Project
1. The project template package comprises three key components:
   * The **DAG template** includes imports and task templates but lacks task dependencies.
   * The **operators** folder with operator templates.
   * A **helper class** for SQL transformations.

With these template files, we executed the DAG successfully.

## Developing Operators
Biult four operators for staging data, transforming data, and performing data quality checks leveraging Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator
Loaded JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator created and ran a SQL COPY statement based on provided parameters, distinguishing between JSON files. It also supported loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators
Utilized SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implemented the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator
Created the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator raise an exception and initiate task retry and eventual failure if test results don't match expectations.


