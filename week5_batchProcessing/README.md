# Intro to Batch Processing using Spark

## Batch vs Streaming

There are 2 ways of processing data:

* ***Batch processing*** : processing *chunks* of data at *regular intervals* .
* Example: processing taxi trips or getting orders into OLAP system each day.
* ***Streaming*** : processing data  *on the fly* .
* Example: processing a taxi trip as soon as it's generated.

![1715125590570](image/README/1715125590570.png)

## Types of batch jobs

A ***batch job*** is a ***job*** (a unit of work) that will process data in batches. Batch jobs can be runned daily, monthly, every 5 minutes, etc and can be carried out using PYthon scrips, SQL, Spark, Flink, etc.

### Orchestrate Batch

To orchestrate the batch jobs, we can use tools like Airflow, prefect, dagster, Mage, etc.

## Pros and cons of batch jobs

* Advantages:
  * Easy to manage. There are multiple tools to manage them (the technologies we already mentioned)
  * Re-executable. Jobs can be easily retried if they fail.
  * Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
* Disadvantages:
  * Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

However, the advantages of batch jobs often compensate for its shortcomings, and as a result most companies that deal with data tend to work with batch jobs mos of the time (probably 90%).

# Spark Introduction

## MacOS spark setup

[Setup mac](https://github.com/yashk1/dataengineering-zoomcamp/blob/main/week5_batchProcessing/MacOS%20setup.md)
