# Analytics Engineering

As the data domain has developed over time, new tools have been introduced that have changed the dynamics of working with data:

Massively parallel processing (MPP) databases
Lower the cost of storage
BigQuery, Snowflake, Redshift...
Data-pipelines-as-a-service
Simplify the ETL process
Fivetran, Stitch...
SQL-first / Version control systems
Looker...
Self service analytics
Mode...
Data governance
The introduction of all of these tools changed the way the data teams work as well as the way that the stakeholders consume the data, creating a gap in the roles of the data team. Traditionally:

The data engineer prepares and maintains the infrastructure the data team needs.
The data analyst uses data to answer questions and solve problems (they are in charge of today).
The data scientist predicts the future based on past patterns and covers the what-ifs rather than the day-to-day (they are in charge of tomorrow).
However, with the introduction of these tools, both data scientists and analysts find themselves writing more code even though they're not software engineers and writing code isn't their top priority. Data engineers are good software engineers but they don't have the training in how the data is going to be used by the business users.

The analytics engineer is the role that tries to fill the gap: it introduces the good software engineering practices to the efforts of data analysts and data scientists. The analytics engineer may be exposed to the following tools:

Data Loading (Stitch...)
Data Storing (Data Warehouses)
Data Modeling (dbt, Dataform...)
Data Presentation (BI tools like Looker, Mode, Tableau...)
This lesson focuses on the last 2 parts: Data Modeling and Data Presentation.
