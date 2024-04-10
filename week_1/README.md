# Week 1 Overview: Fundamentals of Docker, Postgres, and GCP

In the first week of the Data Engineering Zoomcamp 2024 course, the focus is on establishing a solid foundation in the core tools and technologies used in data engineering. The key topics covered during this week include:

## Docker and Postgres

1. **Environment Setup**: Installing and configuring essential tools, such as Python 3, Google Cloud SDK, Docker with docker-compose, Terraform, and Postgres CLI.
   * Ensuring a consistent development environment for the course.
2. **Docker and Postgres Integration**:* Understanding the need for Docker and its role in building data pipelines.
   * Running a Postgres database **locally** using Docker.
   * Connecting to the Postgres database using the pgcli tool.
3. **NY Taxi Data Ingestion**:* Exploring the NY Taxi dataset, which will be used throughout the course.
   * Ingesting the NY Taxi data into the Postgres database.
4. **Connecting pgAdmin and Postgres**:* Introducing the pgAdmin tool for managing and interacting with the Postgres database.
   * Setting up a Docker network to connect pgAdmin and Postgres.
5. **Dockerizing the Ingestion Script**:* Converting a Jupyter notebook into a Python script for data ingestion.
   * Parameterizing the script using argparse.
   * Dockerizing the ingestion script for easier deployment and portability.
6. **Docker-Compose for Multiple Containers**:* Understanding the need for Docker-compose.
   * Configuring a Docker-compose YAML file to run Postgres and pgAdmin simultaneously.
   * Launching the multi-container setup using docker-compose.

## Google Cloud Platform (GCP)

- Getting familiar with the GCP interface and creating a new project.
- Uploading an SSH key for easy access to the GCP resources.
- Launching virtual machines (VMs) and practicing the skills learned in the local environment.
- Destroying the GCP resources once the tasks are completed.

## Terraform

- Introducing the basic usage of Terraform, a popular Infrastructure as Code (IaC) tool.
- Starting with a regular Terraform configuration file, then moving to a variables.tf file for better modularity.
- Practicing the Terraform workflow: plan, apply, and destroy the resources.
