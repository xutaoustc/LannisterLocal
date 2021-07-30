package com.lannister.core.engine.spark.data

import com.lannister.core.domain.ApplicationData

import org.apache.spark.deploy.history.HistoryAppStatusStoreWrapper

class SparkApplicationData(val store: HistoryAppStatusStoreWrapper) extends ApplicationData
