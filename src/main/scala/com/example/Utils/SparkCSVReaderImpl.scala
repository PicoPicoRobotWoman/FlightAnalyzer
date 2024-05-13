package com.example.Utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkCSVReaderImpl(spark: SparkSession) extends SparkCSVReader {

	override def readCsv(path: String): DataFrame = {
		spark
			.read
			.option("inferSchema", "true")
			.option("header", "true")
			.option("sep", ",")
			.option("nullValue", "n/a")
			.csv(path)
	}

}
