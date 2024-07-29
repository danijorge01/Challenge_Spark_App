package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadCsvFiles {

  def main(args: Array[String]): Unit = {

    // Creates a SparkSession
    val spark = SparkSession.builder().appName("Read Csv File").master("local[1]").getOrCreate()

    // Path of the .csv files
    val file_googlePlaystore = "src/main/scala/org/example/csv_files/googleplaystore.csv"
    val file_googleReviews = "src/main/scala/org/example/csv_files/googleplaystore_user_reviews.csv"

    // Read the CSV file
    val df1 = spark.read.option("header", "true").csv(path = file_googleReviews)
    val df2 = spark.read.option("header", "true").csv(path = file_googlePlaystore)

    //////////////////////////////////////////////
    // Part 1

    // Average of the column Sentiment_Polarity grouped by App name
    val df_1 = df1.groupBy(col("App"))
      .agg(avg(col("Sentiment_Polarity").cast("double")).alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))

    df_1.show(5)

    //////////////////////////////////
    // Part 2

    val resultDf2 = df2
      .withColumn("Rating", col("Rating").cast("double"))
      .withColumn("Rating", when(isnan(col("Rating")), lit(0.0)).otherwise(col("Rating")))
      .filter(col("Rating") >= 4.0)
      .orderBy(col("Rating").desc)

    // Show the result
    resultDf2.show(5)

    // Writes the result into a .csv file
    resultDf2.repartition(1)
      .write
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .csv("src/main/outputs/best_apps")

    ///////////////////////////////////
    //Part 3

    // Select and transform the columns
    val transformedDf3 = df2.select(
      col("App").cast(StringType),
      split(col("Category"), ",").cast(ArrayType(StringType)).as("Categories"),
      col("Rating").cast(DoubleType),
      col("Reviews").cast(LongType),
      when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast(DoubleType))
        .when(col("Size").endsWith("k"), (regexp_replace(col("Size"), "k", "").cast(DoubleType) / 1024))
        .otherwise(Double.NaN).as("Size"),
      col("Installs").cast(StringType),
      col("Type").cast(StringType),
      when(col("Price").startsWith("$"), regexp_replace(col("Price"), "\\$", "").cast(DoubleType) * lit(0.9))
        .otherwise(Double.NaN).as("Price"),
      col("Content Rating").cast(StringType).as("Content_Rating"),
      split(col("Genres"), ";").cast(ArrayType(StringType)).as("Genres"),
      to_timestamp(col("Last Updated"), "MMMM d, yyyy").as("Last_Updated"),
      col("Current Ver").cast(StringType).as("Current_Version"),
      col("Android Ver").cast(StringType).as("Minimum_Android_Version")
    )

    val df_3 = transformedDf3
      .withColumn("Rating", coalesce(col("Rating"), lit(Double.NaN)))
      .withColumn("Reviews", coalesce(col("Reviews"), lit(0L)))
      .withColumn("Size", coalesce(col("Size"), lit(Double.NaN)))
      .withColumn("Installs", coalesce(col("Installs"), lit("NaN")))
      .withColumn("Type", coalesce(col("Type"), lit("NaN")))
      .withColumn("Price", coalesce(col("Price"), lit(Double.NaN)))
      .withColumn("Content_Rating", coalesce(col("Content_Rating"), lit("NaN")))
      .withColumn("Genres", coalesce(col("Genres"), array(lit("NaN"))))
      .withColumn("Last_Updated", coalesce(col("Last_Updated"), lit("NaN")))
      .withColumn("Current_Version", coalesce(col("Current_Version"), lit("NaN")))
      .withColumn("Minimum_Android_Version", coalesce(col("Minimum_Android_Version"), lit("NaN")))
      .dropDuplicates("App")

    df_3.show(5)

    //////////////////////////////
    // Part 4

    // Join the DataFrames from Part 1 and Part 3
    val part4 = df_3.join(df_1, Seq("App"), "left_outer")
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))

    part4.show(5)

    // Save the DataFrame as a parquet file with gzip compression
    part4.write
      .option("header", "true")
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet("src/main/outputs/googleplaystore_cleaned")

    ///////////////////////////////
    // Part 5

    // Joins df_1 with df_3 to get the Average_Sentiment_Polarity
    val joinedDf = df_3.join(df_1, df_3("App") === df_1("App"), "left_outer")
      .drop(df_1("App"))

    // Create df_4 with metrics by genre
    val df_4 = joinedDf.withColumn("Genre", explode(col("Genres")))
      .groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        round(avg("Rating"), 2).alias("Average_Rating"),
        round(avg("Average_Sentiment_Polarity"), 2).alias("Average_Sentiment_Polarity")
      )

    // Save df_4 as a parquet file with gzip compression
    df_4.write
      .option("header", "true")
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet("src/main/outputs/googleplaystore_metrics")

    // Show the first few rows of df_4 for verification
    df_4.show(5)
    spark.stop()
  }
}