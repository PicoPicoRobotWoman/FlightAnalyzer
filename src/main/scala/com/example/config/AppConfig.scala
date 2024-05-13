package com.example.config

case class AppConfig(isLocalMode: Boolean,
										 airlinesFilePath: String,
										 airportsFilePath: String,
										 flightsFilePath: String,
										 metaFilePath: String,
										 airportColumns: String,
										 airlineColumns: String,
										 flightColumns: String,
										 targetDirectory: String)
