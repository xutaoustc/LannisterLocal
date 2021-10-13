package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult}

class TasksHeuristic (private val config: HeuristicConfiguration) extends Heuristic {
  override def apply(data: ApplicationData): HeuristicResult = {
//    HD("Spark completed tasks count", evaluator.numCompletedTasks.toString),
//    lazy val numCompletedTasks = jobData.map(_.numCompletedTasks).sum

    null
  }
}
