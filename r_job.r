# Databricks notebook source
# MAGIC %md
# MAGIC # Taxi fares by hour — scheduled R job
# MAGIC Thin wrapper: the logic lives in `taxi_fares.R`, the same file developers edit and run over SSH.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog <- dbutils.widgets.get("catalog")
stopifnot(nzchar(catalog))

# COMMAND ----------

library(sparklyr)
sc <- spark_connect(method = "databricks")

source("taxi_fares.R")
run_taxi_fares(sc, catalog)
