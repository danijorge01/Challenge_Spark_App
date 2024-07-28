# Spark App

This project analyzes Google Play Store data using Apache Spark.

## Input Files

* `googleplaystore.csv`: contains app metadata
* `googleplaystore_user_reviews.csv`: contains user reviews for apps

## Output Files

* `best_apps.csv`: top-rated apps with average sentiment polarity
* `googleplaystore_cleaned.parquet`: cleaned and transformed app data
* `googleplaystore_metrics.parquet`: metrics by genre (count, average rating, average sentiment polarity)

## Running the Project

1. Compile the project using `sbt compile`
2. Run the project using `sbt run`
3. Verify the output files in the `outputs` directory
