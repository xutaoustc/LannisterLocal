package com.ctyun.lannister

import scala.collection.mutable

import com.ctyun.lannister.analysis._
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.conf.aggregator.{AggregatorConfiguration, AggregatorConfigurations}
import com.ctyun.lannister.conf.fetcher.{FetcherConfiguration, FetcherConfigurations}
import com.ctyun.lannister.conf.heuristic.{HeuristicConfiguration, HeuristicConfigurations}
import com.ctyun.lannister.util.{Logging, Utils}
import org.springframework.stereotype.Component


@Component
class LannisterContext extends Logging{
  private val _typeToAggregator = mutable.Map[ApplicationType, MetricsAggregator]()
  private val _typeToFetcher = mutable.Map[ApplicationType, Fetcher[_<:ApplicationData]]()
  private val _typeToHeuristics = mutable.Map[ApplicationType, List[Heuristic]]()
  private val _nameToType = mutable.Map[String, ApplicationType]()

  loadAggregators()
  loadFetchers()
  loadHeuristics()
  configureSupportedApplicationTypes()


  def getApplicationTypeForName(name: String): Option[ApplicationType] = {
    _nameToType.get(name)
  }

  def getFetcherForApplicationType(typ: ApplicationType): Fetcher[_ <: ApplicationData] = {
    _typeToFetcher(typ)
  }

  def getHeuristicsForApplicationType(typ: ApplicationType): List[Heuristic] = {
    _typeToHeuristics(typ)
  }

  def getAggregatorForApplicationType(typ: ApplicationType): MetricsAggregator = {
    _typeToAggregator(typ)
  }



  private def loadAggregators(): Unit = {
    Utils.loadYml(Configs.AGGREGATORS_CONF.getValue)(classOf[AggregatorConfigurations])
      .iterator
      .foreach(conf => {
        val instance = Utils.classForName(conf.classname)
                            .getConstructor(classOf[AggregatorConfiguration])
                            .newInstance(conf).asInstanceOf[MetricsAggregator]
        _typeToAggregator += (conf.getAppType -> instance)
        info(s"Load aggregator ${conf.classname}")
      })
  }

  private def loadFetchers(): Unit = {
    Utils.loadYml(Configs.FETCHERS_CONF.getValue)(classOf[FetcherConfigurations])
      .iterator
      .foreach(conf => {
        val instance = Utils.classForName(conf.classname)
                            .getConstructor(classOf[FetcherConfiguration])
                            .newInstance(conf).asInstanceOf[Fetcher[_<:ApplicationData]]
        _typeToFetcher += (conf.getAppType -> instance)
        info(s"Load fetcher ${conf.classname}")
      })
  }

  private def loadHeuristics(): Unit = {
    Utils.loadYml(Configs.HEURISTICS_CONF.getValue)(classOf[HeuristicConfigurations])
      .iterator
      .foreach(conf => {
        val instance = Utils.classForName(conf.classname)
                            .getConstructor(classOf[HeuristicConfiguration])
                            .newInstance(conf).asInstanceOf[Heuristic]
        val value = _typeToHeuristics.getOrElseUpdate(conf.getAppType, Nil)
        _typeToHeuristics.put(conf.getAppType, value :+ instance)
        info(s"Load heuristic ${conf.classname}")
      })
  }


  private def configureSupportedApplicationTypes(): Unit = {
    val supportedTypes = _typeToFetcher.keySet & _typeToHeuristics.keySet & _typeToAggregator.keySet

    info("Configuring LannisterContext ... ")
    supportedTypes.foreach(eachType => {
      info(s"""Supports ${eachType.upperName} application type,
           |using ${_typeToFetcher(eachType).getClass} fetcher class
           |with Heuristics [ ${_typeToHeuristics(eachType).map(_.getClass).mkString(",")} ]
           |""".stripMargin )
    })

    _typeToFetcher.retain((t, _) => {supportedTypes.contains(t)})
    _typeToHeuristics.retain((t, _) => {supportedTypes.contains(t)})
    _typeToAggregator.retain((t, _) => {supportedTypes.contains(t)})
    supportedTypes.foldLeft(_nameToType)( (m, v) => {m.put(v.upperName, v); m} )
  }

}

