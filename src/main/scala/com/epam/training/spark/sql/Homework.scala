package com.epam.training.spark.sql

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark SQL homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext)

    sc.stop()

  }

  def processData(sqlContext: HiveContext): Unit = {

    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sqlContext, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      * Hint: try to use udf for the null check
      */
    val errors: Array[Row] = findErrors(climateDataFrame)
    println(errors)

    /**
      * Task 3
      * List average temperature for a given day in every year
      */
    val averageTemeperatureDataFrame: DataFrame = averageTemperature(climateDataFrame, 1, 2)

    /**
      * Task 4
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      * Hint: if the dataframe contains a single row with a single double value you can get the double like this "df.first().getDouble(0)"
      */
    val predictedTemperature: Double = predictTemperature(climateDataFrame, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def readCsvData(sqlContext: HiveContext, rawDataPath: String): DataFrame = {
    val df = sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(Constants.CLIMATE_TYPE)
      .csv(rawDataPath)
    df.printSchema()
    val a = df.collect()
    print(a)
    df
  }

  def findErrors(climateDataFrame: DataFrame): Array[Row] = {
    val isNull = udf((value: Any) => {
      if (value == null) 1
      else 0
    })

    climateDataFrame
      .select(
        isNull(col("observation_date")).as("observation_date"),
        isNull(col("mean_temperature")).as("mean_temperature"),
        isNull(col("max_temperature")).as("max_temperature"),
        isNull(col("min_temperature")).as("min_temperature"),
        isNull(col("precipitation_mm")).as("precipitation_mm"),
        isNull(col("precipitation_type")).as("precipitation_type"),
        isNull(col("sunshine_hours")).as("sunshine_hours")
      )
      .agg(
        sum(col("observation_date")),
        sum(col("mean_temperature")),
        sum(col("max_temperature")),
        sum(col("min_temperature")),
        sum(col("precipitation_mm")),
        sum(col("precipitation_type")),
        sum(col("sunshine_hours"))
      ).collect()
  }

  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame = {
    val df = climateDataFrame
      .withColumn("year", year(col("observation_date")))
      .withColumn("month", month(col("observation_date")))
      .withColumn("dayofmonth", dayofmonth(col("observation_date")))

    df.printSchema()

    df.where(col("month") === monthNumber && col("dayOfMonth") === dayOfMonth)
    .select(col("mean_temperature"))
  }

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double = {
    val df = climateDataFrame
      .withColumn("year", year(col("observation_date")))
      .withColumn("month", month(col("observation_date")))
      .withColumn("dayofmonth", dayofmonth(col("observation_date")))

    df.printSchema()

    df.where(col("month") === monthNumber && col("dayOfMonth").isin(dayOfMonth - 1, dayOfMonth, dayOfMonth + 1))
    .agg(avg(col("mean_temperature"))).first().getDouble(0)
  }


}


