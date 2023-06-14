# Formula 1 Analysis with Databricks

This repository contains code and resources for analyzing Formula 1 race results using Databricks. The data used for analysis is sourced from the ergast.com website and is hosted in Azure Datalake.

**Introduction**

Formula 1 is a popular motorsport that attracts millions of fans worldwide. This repository aims to provide a data analysis solution for Formula 1 race results using Databricks. The data is sourced from ergast.com, a website dedicated to Formula 1 statistics, and is stored in Azure Datalake.

Databricks is a powerful data engineering and analytics platform that provides an interactive workspace for running Apache Spark-based applications. By utilizing Databricks, this repository enables you to perform insightful analyses, visualize trends, and gain valuable insights from the Formula 1 race results.

**Problem Statement**

Using the data made available by ergast.com, the solution cleans the data, adds requires formatting and saves the data in parquet format to datalake in silver layer. The source data lies in the same datalake in bronze layer. The data is further analysed and results are published. These results are saved in gold layer. Below are pictures showing 2020 race results and the results processed within datalake and they match.

**Results as seeen in BBC**

<img width="1085" alt="Screenshot 2023-06-11 at 11 35 57 AM" src="https://github.com/iampawankc/databricks_formula1/assets/13145715/c1d4852d-9a46-436b-b36c-7039192f2b53">

**Data bricks processed results**
<img width="1270" alt="Screenshot 2023-06-11 at 11 29 13 AM" src="https://github.com/iampawankc/databricks_formula1/assets/13145715/80a54bc5-a443-47e8-ac31-edb36528af2d">


**Dependencies**

The main dependencies for this project are:
Python 3.x\
Databricks\
Apache Spark\
Azure Datalake Storage SDK
