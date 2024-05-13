package com.example

object FlightAnalyzer extends App with Configure {

	try {

		dataProcessor.process(appConfig)

	}	finally {
		spark.stop()
	}

}