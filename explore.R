# Interactive walkthrough — run line by line, or all at once with source("explore.R", echo = TRUE).
# Runs on the Databricks cluster over SSH; nothing here touches your laptop's R.

# Unity Catalog catalog where you can create a schema; section 5 writes <catalog>.r_demo.taxi_fares_by_hour
catalog <- "main"

suppressPackageStartupMessages({
  library(sparklyr)
  library(dplyr)
})

R.version.string                       # R that ships with the Databricks Runtime
packageVersion("sparklyr")

# ---- 1. Connect to the cluster's Spark ---------------------------------------
# The SSH session already carries DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_CLUSTER_ID,
# so this connects back to the same cluster as you, with Unity Catalog permissions enforced.
sc <- spark_connect(method = "databricks_connect", cluster_id = Sys.getenv("DATABRICKS_CLUSTER_ID"))
DBI::dbGetQuery(sc, "SELECT current_user() AS user, current_catalog() AS catalog")

# ---- 2. Browse governed data in Unity Catalog --------------------------------
DBI::dbGetQuery(sc, "SHOW SCHEMAS IN samples")

trips <- tbl(sc, dbplyr::in_catalog("samples", "nyctaxi", "trips"))
trips                                  # lazy: only the first rows are fetched
sdf_nrow(trips)                        # counted on the cluster

# ---- 3. dplyr runs on Spark, not in R memory ---------------------------------
by_hour <- trips %>%
  filter(trip_distance > 0, fare_amount > 0) %>%
  mutate(pickup_hour = hour(tpep_pickup_datetime)) %>%
  group_by(pickup_hour) %>%
  summarise(trips = n(), avg_fare = round(mean(fare_amount, na.rm = TRUE), 2)) %>%
  arrange(pickup_hour)

show_query(by_hour)                    # the Spark SQL that dplyr generated
collect(by_hour)

# ---- 4. Bring a sample into R for modeling with any CRAN package -------------
sample_df <- trips %>%
  filter(trip_distance > 0, trip_distance < 30, fare_amount > 0, fare_amount < 150) %>%
  select(fare_amount, trip_distance) %>%
  sdf_sample(fraction = 0.2, replacement = FALSE, seed = 42) %>%
  collect()

fit <- lm(fare_amount ~ trip_distance, data = sample_df)
summary(fit)

# ---- 5. Run the production script exactly as the scheduled job will ----------
source("taxi_fares.R")
run_taxi_fares(sc, catalog = catalog)

spark_disconnect(sc)
