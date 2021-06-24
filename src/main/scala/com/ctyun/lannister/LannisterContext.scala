package com.ctyun.lannister

import scala.collection.mutable

import com.ctyun.lannister.analysis._
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.conf.aggregator.{AggregatorConfiguration, AggregatorConfigurationData}
import com.ctyun.lannister.conf.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.ctyun.lannister.conf.heuristic.{HeuristicConfigData, HeuristicConfiguration}
import com.ctyun.lannister.util.{Logging, Utils}
import org.springframework.stereotype.Component


@Component
class LannisterContext extends Logging{

  private val _nameToType = mutable.Map[String, ApplicationType]()
  private val _typeToAggregator = mutable.Map[ApplicationType, MetricsAggregator]()
  private val _typeToFetcher = mutable.Map[ApplicationType, Fetcher[_<:ApplicationData]]()
  private val _typeToHeuristics = mutable.Map[ApplicationType, List[Heuristic]]()

  loadAggregators()
  loadFetchers()
  loadHeuristics()
// TODO loadAutoTuningConf();
  configureSupportedApplicationTypes()


  def getApplicationTypeForName(typ: String): Option[ApplicationType] = _nameToType.get(typ)

  def getAggregatorForApplicationType(applicationType: ApplicationType): MetricsAggregator = {
    _typeToAggregator(applicationType)
  }

  def getFetcherForApplicationType(typ: ApplicationType): Fetcher[_ <: ApplicationData] = {
    _typeToFetcher(typ)
  }

  def getHeuristicsForApplicationType(typ: ApplicationType): List[Heuristic] = {
    _typeToHeuristics(typ)
  }

  private def loadAggregators(): Unit = {
    val aggregatorConfiguration =
      Utils.loadYmlDoc(Configs.AGGREGATORS_CONF.getValue)(classOf[AggregatorConfiguration])

    aggregatorConfiguration.getAggregators.forEach(data => {
      val instance = Utils.classForName(data.classname)
                          .getConstructor(classOf[AggregatorConfigurationData])
                          .newInstance(data).asInstanceOf[MetricsAggregator]
      _typeToAggregator += (data.getAppType -> instance)
      info(s"Load aggregator ${data.classname}")
    })
  }

  private def loadFetchers(): Unit = {
    val fetcherConfiguration =
      Utils.loadYmlDoc(Configs.FETCHERS_CONF.getValue)(classOf[FetcherConfiguration])

    fetcherConfiguration.getFetchers.forEach(data => {
      val instance = Utils.classForName(data.classname)
        .getConstructor(classOf[FetcherConfigurationData])
        .newInstance(data).asInstanceOf[Fetcher[_<:ApplicationData]]
      _typeToFetcher += (data.getAppType -> instance)
      info(s"Load fetcher ${data.classname}")
    })
  }

  private def loadHeuristics(): Unit = {
    val heuristicsConfiguration =
      Utils.loadYmlDoc(Configs.HEURISTICS_CONF.getValue)(classOf[HeuristicConfiguration])

    heuristicsConfiguration.getHeuristics.forEach(data => {
      val instance = Utils.classForName(data.classname)
        .getConstructor(classOf[HeuristicConfigData])
        .newInstance(data).asInstanceOf[Heuristic]
      val value = _typeToHeuristics.getOrElseUpdate(data.getAppType, Nil)

      _typeToHeuristics.put(data.getAppType, value :+ instance)
      info(s"Load heuristic ${data.classname}")
    })
  }


  private def configureSupportedApplicationTypes(): Unit = {
    val supportedTypes = _typeToFetcher.keySet &
      _typeToHeuristics.keySet  &
      _typeToAggregator.keySet

    _typeToFetcher.retain((t, _) => {supportedTypes.contains(t)})
    _typeToHeuristics.retain((t, _) => {supportedTypes.contains(t)})
    _typeToAggregator.retain((t, _) => {supportedTypes.contains(t)})
    supportedTypes.foldLeft(_nameToType)( (m, v) => {m.put(v.upperName, v); m} )

    info("Configuring LannisterContext ... ")
    supportedTypes.foreach(tpe => {
      info(
        s"""Supports ${tpe.upperName} application type,
           |using ${_typeToFetcher(tpe).getClass} fetcher class
           |with Heuristics [ ${_typeToHeuristics(tpe).map(_.getClass).mkString(",")} ]
           |""".stripMargin )
    })
  }

}

