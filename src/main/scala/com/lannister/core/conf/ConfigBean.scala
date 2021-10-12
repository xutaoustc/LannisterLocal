package com.lannister.core.conf

import java.util

import scala.beans.BeanProperty


class AggregatorConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}
class AggregatorConfigurations {
  @BeanProperty var aggregators: util.ArrayList[AggregatorConfiguration] = _

  def iterator: Iterator[AggregatorConfiguration] = {
    import collection.JavaConverters._
    aggregators.iterator().asScala
  }
}

class FetcherConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}
class FetcherConfigurations extends Iterable[FetcherConfiguration] {
  @BeanProperty var fetchers: util.ArrayList[FetcherConfiguration] = _

  def iterator: Iterator[FetcherConfiguration] = {
    import collection.JavaConverters._
    fetchers.iterator().asScala
  }
}

class HeuristicConfiguration {
  @BeanProperty var name: String = _
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}
class HeuristicConfigurations extends Iterable[HeuristicConfiguration]{
  @BeanProperty var heuristics: util.ArrayList[HeuristicConfiguration] = _

  def iterator: Iterator[HeuristicConfiguration] = {
    import collection.JavaConverters._
    heuristics.iterator().asScala
  }
}
