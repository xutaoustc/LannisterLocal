package com.lannister.core.engine.spark.fetchers

import com.lannister.core.domain.ApplicationData

import org.apache.spark.deploy.history.HistoryAppStatusStoreWrapper

case class SparkApplicationData(store: HistoryAppStatusStoreWrapper) extends ApplicationData
