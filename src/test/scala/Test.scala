import com.ctyun.lannister.analysis.{AnalyticJob, ApplicationData, ApplicationType, Fetcher, Heuristic, MetricsAggregator}
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.conf.aggregator.{AggregatorConfiguration, AggregatorConfigurationData}
import com.ctyun.lannister.conf.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.ctyun.lannister.conf.heuristic.{HeuristicConfiguration, HeuristicConfigurationData}
import com.ctyun.lannister.conf.jobtype.JobTypeConfiguration
import com.ctyun.lannister.util.Utils
import com.ctyun.lannister.util.Utils.getClass
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import java.util.concurrent.{Executor, Executors, ThreadFactory}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object Test {
  private val _typeToAggregator = mutable.Map[ApplicationType, MetricsAggregator]()
  private val _typeToFetcher = mutable.Map[ApplicationType, Fetcher]()
  private val _typeToHeuristics = mutable.Map[ApplicationType, List[Heuristic[_ <: ApplicationData]]]()

  def main(args: Array[String]): Unit = {

    val map = mutable.Map[String,String]()
    map += ("1"->"2")
    map += ("2"->"3")


    val cc = map.retain((x,y)=>{x=="1"})
    println("ss")
  }
}
