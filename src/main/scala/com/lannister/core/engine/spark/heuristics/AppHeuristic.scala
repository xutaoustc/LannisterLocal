package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.engine.spark.heuristics.AppHeuristic.Evaluator

class AppHeuristic (private val config: HeuristicConfiguration) extends Heuristic{
  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Total input bytes", evaluator.inputBytesTotal.toString),
      HD("Total output bytes", evaluator.outputBytesTotal.toString),
      HD("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      HD("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString)
    )

    HR(config.classname, config.name, Severity.NONE, 0, hds.toList)
  }
}

object AppHeuristic {

  class Evaluator(heuristic: AppHeuristic, data: SparkApplicationData) {
    lazy val stageData = data.store.store.stageList(null)

    lazy val inputBytesTotal = stageData.map(_.inputBytes).sum
    // tasks field in StageData is None, we can not use it to compute sum value
    lazy val outputBytesTotal = stageData.map(_.outputBytes).sum
    lazy val shuffleReadBytesTotal = stageData.map(_.shuffleReadBytes).sum
    lazy val shuffleWriteBytesTotal = stageData.map(_.shuffleWriteBytes).sum
  }
}
