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


# 5.4 Anatomy of Spark Cluster


#### Spark Cluster

**Spark Execution modes** : It is possible to run a spark application using  **cluster mode** , **local mode** (pseudo-cluster) or with an **interactive** shell (*pypsark* or  *spark-shell* ).

So far we’ve used a **local cluster** to run our Spark code, but Spark **clusters** often contain multiple computers that act as  **executors** .

Spark clusters are managed by a  **master** , which behaves similarly to an entry point to a Kubernetes cluster. A **driver** (an Airflow DAG, a computer running a local script, etc.) who wants to run a Spark job will send the job to the master, who in turn will distribute the work among the  **executors in the cluster** . If an executor fails and becomes offline for any reason, the master will reassign the task to another executor.

Using cluster mode:

* Spark applications are run as independent sets of processes, coordinated by a SparkContext object in your main program (called the  *driver program* ).
* The *context* connects to the cluster manager  *which allocate resources* .
* Each *worker* in the cluster is managed by an  *executor* .
* The *executor* manages computation as well as storage and caching on each machine.
* The application code is sent from the *driver* to the executors, and the executors specify the context and the various *tasks* to be run.
* The *driver* program must listen for and accept incoming connections from its executors throughout its lifetime
