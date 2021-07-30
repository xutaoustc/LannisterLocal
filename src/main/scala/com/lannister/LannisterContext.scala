package com.lannister

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.lannister.analysis.Aggregator
import com.lannister.core.conf.Configs
import com.lannister.core.conf.aggregator.{AggregatorConfiguration, AggregatorConfigurations}
import com.lannister.core.conf.fetcher.{FetcherConfiguration, FetcherConfigurations}
import com.lannister.core.conf.heuristic.{HeuristicConfiguration, HeuristicConfigurations}
import com.lannister.core.domain.{ApplicationData, Fetcher, Heuristic}
import com.lannister.core.util.{Logging, Utils}
import org.springframework.stereotype.Component


@Component
class LannisterContext extends Logging{
  private val _typeToAggregator = mutable.Map[String, Aggregator]()
  private val _typeToFetcher = mutable.Map[String, Fetcher[_<:ApplicationData]]()
  private val _typeToHeuristics = mutable.Map[String, ListBuffer[Heuristic]]()
  private val _typeSet = mutable.Set[String]()

  loadAggregators()
  loadFetchers()
  loadHeuristics()
  configureSupportedApplicationTypes()


  def applicationTypeSupported(applicationType: String): Boolean = {
    _typeSet.contains(applicationType.toUpperCase())
  }

  def getFetcherForApplicationType(applicationType: String): Fetcher[_ <: ApplicationData] = {
    _typeToFetcher(applicationType.toUpperCase())
  }

  def getHeuristicsForApplicationType(applicationType: String): List[Heuristic] = {
    _typeToHeuristics(applicationType.toUpperCase()).toList
  }

  def getAggregatorForApplicationType(applicationType: String): Aggregator = {
    _typeToAggregator(applicationType.toUpperCase())
  }



  private def loadAggregators(): Unit = {
    Utils.loadYml(Configs.AGGREGATORS_CONF.getValue)(classOf[AggregatorConfigurations])
      .iterator
      .foreach(conf => {
        val instance = Utils.classForName(conf.classname)
                            .getConstructor(classOf[AggregatorConfiguration])
                            .newInstance(conf).asInstanceOf[Aggregator]
        _typeToAggregator += (conf.applicationType.toUpperCase() -> instance)
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
        _typeToFetcher += (conf.applicationType.toUpperCase() -> instance)
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
        val applicationType = conf.applicationType.toUpperCase()
        _typeToHeuristics.getOrElseUpdate(applicationType, ListBuffer()) += instance
        info(s"Load heuristic ${conf.classname}")
      })
  }


  private def configureSupportedApplicationTypes(): Unit = {
    val supportedTypes = _typeToFetcher.keySet & _typeToHeuristics.keySet & _typeToAggregator.keySet

    info("Configuring LannisterContext ... ")
    supportedTypes.foreach(eachType => {
      info(s"""Supports $eachType application type,
           |using ${_typeToFetcher(eachType).getClass} fetcher class
           |with Heuristics [ ${_typeToHeuristics(eachType).map(_.getClass).mkString(",")} ]""" )
    })

    _typeToFetcher.retain((t, _) => supportedTypes.contains(t))
    _typeToHeuristics.retain((t, _) => supportedTypes.contains(t))
    _typeToAggregator.retain((t, _) => supportedTypes.contains(t))
    supportedTypes.foldLeft(_typeSet)( (s, v) => {s.add(v); s} )
  }

}

