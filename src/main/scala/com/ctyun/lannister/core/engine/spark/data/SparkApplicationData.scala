package com.ctyun.lannister.core.engine.spark.data

import com.ctyun.lannister.core.domain.ApplicationData

import org.apache.spark.deploy.history.HistoryAppStatusStoreWrapper

class SparkApplicationData(val store: HistoryAppStatusStoreWrapper) extends ApplicationData
