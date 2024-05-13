package com.example

import com.example.Utils.{SparkCSVReader, SparkCSVReaderImpl, SparkCSVWriter, SparkCSVWriterImpl}
import com.example.config.AppConfig
import com.example.model.DataProcessorImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import pureconfig.ConfigSource
import pureconfig.generic.auto._

trait Configure {

	lazy val appName = "FlightAnalyzer"
	lazy val appConfig: AppConfig = ConfigSource.default.loadOrThrow[AppConfig]
	lazy val spark: SparkSession = getSparkSession(appConfig.isLocalMode)
	lazy val sc: SparkContext = spark.sparkContext

	lazy val sparkCSVReader: SparkCSVReader = new SparkCSVReaderImpl(spark)
	lazy val sparkCSVWriter: SparkCSVWriter = new SparkCSVWriterImpl()
	lazy val dataProcessor: DataProcessorImpl = new DataProcessorImpl(spark, sparkCSVReader, sparkCSVWriter, sc)

	private def getSparkSession(isLocalMode: Boolean): SparkSession = {
		val conf = new SparkConf()
			.setAppName(appName)

		if (isLocalMode) conf.setMaster("local[*]")

		SparkSession
			.builder()
			.config(conf)
			.config("spark.sql.autoBroadcastJoinThreshold", -1)
			.getOrCreate()
	}


}


