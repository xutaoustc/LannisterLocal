package com.lannister.core.conf.heuristic

import java.util

import scala.beans.BeanProperty

class HeuristicConfiguration {
  @BeanProperty var name: String = _
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var viewname: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}


class HeuristicConfigurations extends Iterable[HeuristicConfiguration]{
  @BeanProperty var heuristics: util.ArrayList[HeuristicConfiguration] = _

  def iterator: Iterator[HeuristicConfiguration] = {
    import collection.JavaConverters._
    heuristics.iterator().asScala
  }
}
