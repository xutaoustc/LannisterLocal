package com.lannister.core.conf.heuristic

import java.util

import scala.beans.BeanProperty

class HeuristicConfigurations extends Iterable[HeuristicConfiguration]{
  @BeanProperty var heuristics: util.ArrayList[HeuristicConfiguration] = _

  def iterator: Iterator[HeuristicConfiguration] = {
    import collection.JavaConverters._
    heuristics.iterator().asScala
  }
}
