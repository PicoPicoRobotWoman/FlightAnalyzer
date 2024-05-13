package com.example.Utils

import org.apache.spark.sql.{DataFrame, SaveMode}

trait SparkCSVWriter {

	def write(df: DataFrame, path: String, saveMode: SaveMode): Unit

}
