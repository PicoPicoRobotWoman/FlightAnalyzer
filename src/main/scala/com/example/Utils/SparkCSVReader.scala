package com.example.Utils

import org.apache.spark.sql.DataFrame

trait SparkCSVReader {
	def  readCsv(path: String): DataFrame
}
