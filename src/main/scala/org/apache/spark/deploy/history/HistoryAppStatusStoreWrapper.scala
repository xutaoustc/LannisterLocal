package org.apache.spark.deploy.history

case class HistoryAppStatusStoreWrapper(store: HistoryAppStatusStore)

object HistoryAppStatusStoreWrapper{
  implicit def store2Wrapper(s : HistoryAppStatusStore) : HistoryAppStatusStoreWrapper =
    HistoryAppStatusStoreWrapper(s)
}
