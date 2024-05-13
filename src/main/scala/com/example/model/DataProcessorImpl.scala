package com.example.model
import com.example.Utils._
import com.example.config.AppConfig
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Date
import scala.util.Try

class DataProcessorImpl(spark: SparkSession,
												sparkCSVReader: SparkCSVReader,
												sparkCSVWriter: SparkCSVWriter,
												sc: SparkContext) extends DataProcessor with Logging {

	private val airportPrefix = "airport"
	private val airlinePrefix = "airline"

	override def process(appConfig: AppConfig): Unit = {

		val rawAirportsDf = sparkCSVReader.readCsv(appConfig.airportsFilePath)
		val rawAirlinesDf = sparkCSVReader.readCsv(appConfig.airlinesFilePath)
		val rawFlightsDf = sparkCSVReader.readCsv(appConfig.flightsFilePath)

		val airportsDf = renameColumns(rawAirportsDf, appConfig.airportColumns.split(",").map(_.trim))
		val airlinesDf = renameColumns(rawAirlinesDf, appConfig.airlineColumns.split(",").map(_.trim))
		val flightsDf = renameColumns(rawFlightsDf, appConfig.flightColumns.split(",").map(_.trim))

		val maxDate = flightsDf
			.select(
				to_date(
					max(
						concat(col("YEAR"), lit("-"), lpad(col("MONTH"), 2, "0"), lit("-"), lpad(col("DAY"), 2, "0")
						)
					),
					"yyyy-MM-dd"
				)
			)
			.first()
			.getDate(0)

		val lastDate = Try (
			sparkCSVReader.readCsv(appConfig.metaFilePath)
				.select(max(col("DATE")))
				.first()
				.getDate(0)
		)
			.getOrElse(new Date(0, 0, 0))

		val filterFlightsDF = filterByDte(flightsDf, lastDate)
		val prefAirportDf = addPrefixToAllColumns(airportsDf, airportPrefix)
		val prefAirlineDf = addPrefixToAllColumns(airlinesDf, airlinePrefix)

		val megazordDF = calcMegazord(filterFlightsDF, prefAirportDf, prefAirlineDf)

		val topAirports = megazordDF
			.transform(calcTopAirports)
		sparkCSVWriter.write(topAirports, f"${appConfig.targetDirectory}topAirports.csv", SaveMode.Append)

		val topAirlineDf = megazordDF
			.transform(calcTopAirlines)
		sparkCSVWriter.write(topAirlineDf, f"${appConfig.targetDirectory}topAirlineDf.csv", SaveMode.Append)

		val topAirportByAIRLINE = megazordDF
			.transform(calcTopAirportByAIRLINE)
		sparkCSVWriter.write(topAirportByAIRLINE, f"${appConfig.targetDirectory}topAirportByAIRLINE.csv", SaveMode.Append)

		val topDelayByWeek = megazordDF
			.transform(calcTopDelayByWeek)
		sparkCSVWriter.write(topDelayByWeek, f"${appConfig.targetDirectory}topDelayByWeek.csv", SaveMode.Append)

		val countsByDelay = megazordDF
			.transform(calcCountsByDelay)
		sparkCSVWriter.write(countsByDelay, f"${appConfig.targetDirectory}countsByDelay.csv", SaveMode.Append)

		val sharedMinutesByDelay = megazordDF
			.transform(calcShareMinutes)
		sparkCSVWriter.write(sharedMinutesByDelay, f"${appConfig.targetDirectory}sharedMinutesByDelay.csv", SaveMode.Append)

		sparkCSVWriter.write(
			spark.sql(f"select ${maxDate} as DATE"),
			appConfig.metaFilePath,
			SaveMode.Append)

	}

	private def calcMegazord(flightsDf: DataFrame,
													 airportsDf: DataFrame,
													 airlinesDf: DataFrame): DataFrame = {
		flightsDf
			.join(broadcast(airportsDf), col("ORIGIN_AIRPORT") === col(f"${airportPrefix}_IATA_CODE"))
			.join(broadcast(airlinesDf), col("AIRLINE") === col(s"${airlinePrefix}_IATA_CODE"))
			.cache()

	}

	private def calcTopAirports(df: DataFrame): DataFrame = {

		df
			.groupBy(f"${airportPrefix}_AIRPORT")
			.agg(count("*").as("COUNT"))
			.orderBy(col("COUNT").desc)
			.limit(10)

	}

	private def calcTopAirlines(df: DataFrame): DataFrame = {

		df
			.where(col("CANCELLED") === 0)
			.where(col("DIVERTED") === 0)
			.where(col("AIR_SYSTEM_DELAY").isNull)
			.where(col("SECURITY_DELAY").isNull)
			.where(col("AIRLINE_DELAY").isNull)
			.where(col("LATE_AIRCRAFT_DELAY").isNull)
			.where(col("WEATHER_DELAY").isNull)
			.groupBy(col(f"${airlinePrefix}_AIRLINE"))
			.agg(count("*").as("COUNT"))
			.orderBy(col("COUNT").desc)
			.limit(10)

	}

	private def calcTopAirportByAIRLINE(df: DataFrame): DataFrame = {


		val windowSpec = Window.partitionBy(f"${airportPrefix}_AIRPORT").orderBy(col("COUNT").desc)

		df
			.where(col("DEPARTURE_DELAY") <= 0)
			.groupBy(col(f"${airportPrefix}_AIRPORT"), col("AIRLINE"))
			.agg(count("*").as("COUNT"))
			.withColumn("rank", dense_rank().over(windowSpec))
			.where(col("rank") <= 10)
			.drop("rank")

	}

	private def calcTopDelayByWeek(df: DataFrame): DataFrame = {

		df
			.where(col("ARRIVAL_DELAY") <= 0)
			.groupBy(col("DAY_OF_WEEK"))
			.agg(count("*").as("COUNT"))
			.orderBy(col("COUNT").desc)

	}

	private def calcCountsByDelay(df: DataFrame): DataFrame = {

		val columnsToCount = Seq("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")

		val countNotNull = columnsToCount
			.map(colName => col(colName))
			.map(
				col => sum(when(col.isNotNull, lit(1))
				.otherwise(lit(0)))
				.alias(col.toString)
			)

		df
			.agg(countNotNull.head, countNotNull.tail: _*)

	}

	private def calcShareMinutes(df: DataFrame): DataFrame = {

		val delayColumns = Seq("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")

		def safeSum(col: Column): Column = coalesce(col, lit(0))

		val totalDelayMinutes = delayColumns
			.map(
				colName => safeSum(sum(col(colName)))
			)
			.reduce((total, current) => total + current)

		val sumDelayMinutes = delayColumns.map(colName => safeSum(sum(col(colName))).alias(s"sum_$colName"))

		val delayPercentages = sumDelayMinutes.map(col => {
			val percentage = (col / totalDelayMinutes)
			percentage.alias(s"${col.toString.split("sum_")(1)}_SHARE")
		})

		df
			.select(delayPercentages: _*)

	}

	private def renameColumns(df: DataFrame, expectedColumnNames: Seq[String]): DataFrame = {
		df
			.columns
			.zip(expectedColumnNames)
			.foldLeft(df) { (tempDF, names) =>
				val (actualName, expectedName) = names
				if (actualName != expectedName) {
					tempDF.withColumnRenamed(actualName, expectedName)
				} else {
					tempDF
				}
			}
	}

	private def filterByDte(df: DataFrame, date: Date): DataFrame = {
		df
			.where(
				to_date(concat(col("YEAR"), lit("-"), lpad(col("MONTH"), 2, "0"), lit("-"), lpad(col("DAY"), 2, "0")), "yyyy-MM-dd") > date
			)

	}

	private def addPrefixToAllColumns(df: DataFrame, prefix: String): DataFrame = {
		val columns = df.columns

		var renamedDf = df
		columns
			.foreach {
				colName => renamedDf = renamedDf.withColumnRenamed(colName, s"${prefix}_${colName}")
			}

		renamedDf
	}

}
