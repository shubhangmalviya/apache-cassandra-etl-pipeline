#  Data Modeling with Cassandra  

This project aims to analyse the data collection on songs and user activity on music streaming app. This is particularly useful for the analysis team in understanding what songs users are listening to. The project starts with the state that there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.  
  
We need to create an Apache Cassandra database which can create queries on song play data to answer the questions. We will create a database for this analysis. 
  
## Project Overview
  
In this project, we'll apply the data modeling with Apache Cassandra and complete an ETL pipeline using Python. We will need to model data by creating tables in Apache Cassandra to run queries. The first part of the ETL pipeline  transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.  This first part takes care of all the imports and provides a structure for ETL pipeline you'd need to process this data.  
  
### Datasets  

For this project, We'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:  
  ```
event_data/2018-11-08-events.csv  
event_data/2018-11-09-events.csv  
```
### Project Structure  

To get started with the project, please go to the Jupyter notebook file, in which:  
  
1. We will process the `event_datafile_new.csv` dataset to create a denormalized dataset  
2. We will model the data tables keeping in mind the queries we need to run  
Few queries are written after which we will need to model our data tables and will load the data into tables created in Apache Cassandra and run queries  

### Project Details  

Below are key steps performed
  
1. Modeling NoSQL database or Apache Cassandra database  
2. Design tables to answer the queries outlined in the project template  
3. Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements  
4. Develop CREATE statement for each of the tables to address each queries  
5. Load the data with INSERT statement for each of the tables  

> **Note:** It is recommended that while running the notebooks you can use DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test ETL pipelines
 
### Building ETL Pipeline  

1. Iterate through each event file in event_data to process and create a new CSV file in Python  
2. Include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model  
3. Test by running SELECT statements after running the queries on your database