# NYC taxi fare analysis — the same file runs interactively over SSH and as a scheduled job.
#
#   Over SSH:   Rscript taxi_fares.R <catalog>
#               (connects with sparklyr's databricks_connect method, using the
#                DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_CLUSTER_ID the SSH session provides)
#   In a job:   the r_job notebook connects with method = "databricks", source()s this file,
#               and calls run_taxi_fares()

suppressPackageStartupMessages({
  library(sparklyr)
  library(dplyr)
})

run_taxi_fares <- function(sc, catalog) {
  out_schema <- paste0(catalog, ".r_demo")
  out_table  <- paste0(out_schema, ".taxi_fares_by_hour")

  # 1. Read a governed Unity Catalog table; nothing is copied to the R session yet
  trips <- tbl(sc, dbplyr::in_catalog("samples", "nyctaxi", "trips"))

  # 2. dplyr verbs are translated to Spark SQL and run on the cluster
  by_hour <- trips %>%
    filter(trip_distance > 0, fare_amount > 0) %>%
    mutate(pickup_hour = hour(tpep_pickup_datetime)) %>%
    group_by(pickup_hour) %>%
    summarise(
      trips          = n(),
      avg_fare       = round(mean(fare_amount, na.rm = TRUE), 2),
      avg_distance   = round(mean(trip_distance, na.rm = TRUE), 2),
      fare_per_mile  = round(sum(fare_amount, na.rm = TRUE) / sum(trip_distance, na.rm = TRUE), 2)
    ) %>%
    arrange(pickup_hour)

  # 3. Pull a sample into local R memory and fit a model with base R
  sample_df <- trips %>%
    filter(trip_distance > 0, trip_distance < 30, fare_amount > 0, fare_amount < 150) %>%
    select(fare_amount, trip_distance) %>%
    sdf_sample(fraction = 0.2, replacement = FALSE, seed = 42) %>%
    collect()

  fit <- lm(fare_amount ~ trip_distance, data = sample_df)
  cat(sprintf("Model on %d trips: fare = %.2f + %.2f * miles (R^2 = %.3f)\n",
              nrow(sample_df), coef(fit)[1], coef(fit)[2], summary(fit)$r.squared))

  # 4. Write the result back to Unity Catalog so BI tools, Genie, and other jobs can use it
  DBI::dbExecute(sc, paste0("CREATE SCHEMA IF NOT EXISTS ", out_schema))
  spark_write_table(by_hour, out_table, mode = "overwrite")
  cat("Wrote", out_table, "\n")

  print(collect(by_hour), n = 24)
  invisible(out_table)
}

# Entry point when run as a script (e.g. `Rscript taxi_fares.R my_catalog` over SSH)
if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 1) stop("usage: Rscript taxi_fares.R <catalog>")
  sc <- spark_connect(method = "databricks_connect", cluster_id = Sys.getenv("DATABRICKS_CLUSTER_ID"))
  run_taxi_fares(sc, args[[1]])
  spark_disconnect(sc)
}
