package com.epam.training.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    val sparkSession = org.apache.spark.sql.SparkSession.builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate

    processData(sparkSession)

    sparkSession.stop()

  }

  def processData(sparkSession: SparkSession): Unit = {

    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sparkSession, Homework.RAW_BUDAPEST_DATA)

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

  def readCsvData(sparkSession: SparkSession, rawDataPath: String): DataFrame = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(Constants.CLIMATE_TYPE)
    .csv(rawDataPath)

  def findErrors(climateDataFrame: DataFrame): Array[Row] = {

    def countNullValuesColumn(column: Column): Column =
      count("*") - count(column)

    def countNullValuesColumnArray: Array[Column] =
      climateDataFrame
        .columns
        .map(colName => countNullValuesColumn(col(colName).alias("countOfNullsIn" + colName)))

    climateDataFrame.select(countNullValuesColumnArray: _*).collect()
  }

  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame = climateDataFrame
    .filter(observedOnColumn(monthNumber, dayOfMonth))
    .select("mean_temperature")

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double = climateDataFrame
    .filter(observedOnPlusMinusOneDayColumn(monthNumber, dayOfMonth))
    .select(avg(col("mean_temperature")))
    .first()
    .getDouble(0)

  private val observedOnColumn = col("observation_date")
  private val dayBeforeObserveColumn = date_sub(observedOnColumn, 1)
  private val dayAfterObserveColumn = date_add(observedOnColumn, 1)

  private def observedOnColumn(monthNumber: Int, dayOfMonth: Int) : Column =
    (month(observedOnColumn) === monthNumber)
      .&&(dayofmonth(observedOnColumn) === dayOfMonth)

  private def observedOnPlusMinusOneDayColumn(monthNumber: Int, dayOfMonth: Int) : Column=
    (month(observedOnColumn) === monthNumber && dayofmonth(observedOnColumn) === dayOfMonth)
      .|| (month(dayBeforeObserveColumn) === monthNumber && dayofmonth(dayBeforeObserveColumn) === dayOfMonth)
      .|| (month(dayAfterObserveColumn) === monthNumber && dayofmonth(dayAfterObserveColumn) === dayOfMonth)
}


