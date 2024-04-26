# Data Warehousing on GCS using Terraform and Mage

## Aim

[Week 3 of DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse) explains the details of data warehousing in Google Cloud, including the side of costs, and how to reduce them using partitioning and clustering.

The purpose of the homework is to create a data warehouse in GCS containing parquet file from the NY taxi data available [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## OLAP VS OLTP

In Data science, when we're discussing data processing systems, there are 2 main types: **OLAP** and **OLTP** systems.

* ***OLTP*** : Online Transaction Processing.
* ***OLAP*** : Online Analytical Processing.

An intuitive way of looking at both of these systems is that OLTP systems are "classic databases" whereas OLAP systems are catered for advanced data analytics purposes.

|                     | OLTP                                                                                              | OLAP                                                                              |
| ------------------- | ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Purpose             | Control and run essential business operations in real time                                        | Plan, solve problems, support decisions, discover hidden insights                 |
| Data updates        | Short, fast updates initiated by user                                                             | Data periodically refreshed with scheduled, long-running batch jobs               |
| Database design     | Normalized databases for efficiency                                                               | Denormalized databases for analysis                                               |
| Space requirements  | Generally small if historical data is archived                                                    | Generally large due to aggregating large datasets                                 |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Productivity        | Increases productivity of end users                                                               | Increases productivity of business managers, data analysts and executives         |
| Data view           | Lists day-to-day business transactions                                                            | Multi-dimensional view of enterprise data                                         |
| User examples       | Customer-facing personnel, clerks, online shoppers                                                | Knowledge workers such as data analysts, business analysts and executives         |

# What is a Data Warehouse?

A Data Warehouse (DW) is an ***OLAP solution*** meant for ***reporting and data analysis*** . Unlike Data Lakes, which follow the ELT model, DWs commonly use the ETL model.

A DW receives data from different ***data sources*** which is then processed in a ***staging area*** before being ingested to the actual warehouse (a database) and arranged as needed. DWs may then feed data to separate  ***Data Marts*** ; smaller database systems which end users may use for different purposes.

[![dw arch](https://github.com/ziritrion/dataeng-zoomcamp/raw/main/notes/images/03_01.jpeg)](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/images/03_01.jpeg)

## Data Lake vs Data warehouse vs Data Lakehouse

1. **Data Warehouse** : A data warehouse is a centralized repository that stores structured data from various sources. It's designed for query and analysis rather than transaction processing. Data warehouses are typically used for business intelligence (BI) activities such as reporting, data analysis, and decision-making. They are optimized for complex queries and often employ techniques like indexing and denormalization to enhance performance.
2. **Data Lake** : A data lake is a vast pool of raw data, structured, semi-structured, or unstructured, stored in its native format. Unlike a data warehouse, which imposes structure before data is stored, a data lake allows for the storage of diverse data types without prior transformation. Data lakes are used for exploratory analysis, machine learning, and big data processing, offering flexibility and scalability for storing large volumes of data.
3. **Data Lakehouse** : A data lakehouse combines the capabilities of both a data lake and a data warehouse, offering the flexibility of a data lake with the reliability and performance of a data warehouse. It integrates structured and unstructured data in a centralized repository, allowing for both real-time analytics and traditional BI queries. Data lakehouses leverage modern technologies like Apache Spark and Delta Lake to provide ACID transactions, schema enforcement, and data quality features on top of the data lake infrastructure. This hybrid approach aims to address the shortcomings of traditional data warehouses and data lakes.

# BigQuery

BigQuery (BQ) is a **Data Warehouse** solution offered by Google Cloud Platform.

* BQ is ***serverless*** . There are no servers to manage or database software to install; this is managed by Google and it's transparent to the customers.
* BQ is ***scalable*** and has  ***high availability*** . Google takes care of the underlying software and infrastructure.
* BQ has built-in features like Machine Learning, Geospatial Analysis and Business Intelligence among others.
* BQ maximizes flexibility by separating data analysis and storage in **different *compute engines*** , thus allowing the customers to budget accordingly and reduce costs.

Some alternatives to BigQuery from other cloud providers would be AWS Redshift or Azure Synapse Analytics.
