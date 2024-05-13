package com.example.Utils
import org.apache.spark.sql.{DataFrame, SaveMode}

class SparkCSVWriterImpl extends SparkCSVWriter {

	override def write(df: DataFrame, path: String, saveMode: SaveMode): Unit = {

		df
			.write
			.mode(saveMode)
			.csv(path)

	}

}
