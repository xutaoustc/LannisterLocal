package com.ctyun.lannister.core.engine.spark.data

import com.ctyun.lannister.analysis.ApplicationData

import org.apache.spark.deploy.history.{HistoryAppStatusStoreWrapper}

class SparkApplicationData(val store: HistoryAppStatusStoreWrapper) extends ApplicationData{
  override def isEmpty: Boolean = false
}
