package com.example.model

import com.example.config.AppConfig

trait DataProcessor {

	def process(appConfig: AppConfig): Unit

}
