package com.ctyun.lannister.conf.heuristic

import java.util
import scala.beans.BeanProperty

class HeuristicConfiguration {
  @BeanProperty var heuristics: util.ArrayList[HeuristicConfigurationData] = _

}
