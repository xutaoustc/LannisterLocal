package com.ctyun.lannister

import com.ctyun.lannister.analysis.{ApplicationData, ApplicationType, Fetcher, Heuristic, JobType, MetricsAggregator}
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.conf.aggregator.{AggregatorConfiguration, AggregatorConfigurationData}
import com.ctyun.lannister.conf.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.ctyun.lannister.conf.heuristic.{HeuristicConfiguration, HeuristicConfigurationData}
import com.ctyun.lannister.conf.jobtype.JobTypeConfiguration
import com.ctyun.lannister.dao.{AppHeuristicResultDao, AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.util.{Logging, Utils}
import org.apache.hadoop.conf.Configuration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

@Component
class LannisterContext extends Logging{
  @Autowired
  var appResultDao: AppResultDao = _
  @Autowired
  var appHeuristicResultDao: AppHeuristicResultDao = _
  @Autowired
  var appHeuristicResultDetailsDao: AppHeuristicResultDetailsDao = _

  private var hadoopConf:Configuration = _

  private val _nameToType = mutable.Map[String,ApplicationType]()
  private val _typeToAggregator = mutable.Map[ApplicationType, MetricsAggregator]()
  private val _typeToFetcher = mutable.Map[ApplicationType, Fetcher[_<:ApplicationData]]()
  private val _typeToHeuristics = mutable.Map[ApplicationType, List[Heuristic]]()
  private val _appTypeToJobTypes = mutable.Map[ApplicationType, List[JobType]]()

  loadAggregators
  loadFetchers
  loadHeuristics
  loadJobTypes
  loadGeneralConf
// TODO loadAutoTuningConf();
  configureSupportedApplicationTypes


  def getApplicationTypeForName(typeName: String) = _nameToType(typeName)

  def getConfiguration = hadoopConf

  def getAggregatorForApplicationType(applicationType: ApplicationType)={
    _typeToAggregator(applicationType)
  }

  def getFetcherForApplicationType(applicationType: ApplicationType)={
    _typeToFetcher(applicationType)
  }

  def getHeuristicsForApplicationType(applicationType: ApplicationType)={
    _typeToHeuristics(applicationType)
  }

  private def loadAggregators={
    val aggregatorConfiguration = Utils.loadYmlDoc(Configs.AGGREGATORS_CONF.getValue)(classOf[AggregatorConfiguration])

    aggregatorConfiguration.getAggregators.forEach(data=>{
      val instance = Class.forName(data.classname)
                          .getConstructor(classOf[AggregatorConfigurationData]).newInstance(data).asInstanceOf[MetricsAggregator]
      _typeToAggregator += (data.getAppType -> instance)
      info(s"Load aggregator ${data.classname}")
    })
  }

  private def loadFetchers={
    val fetcherConfiguration = Utils.loadYmlDoc(Configs.FETCHERS_CONF.getValue)(classOf[FetcherConfiguration])

    fetcherConfiguration.getFetchers.forEach(data=>{
      val instance = Class.forName(data.classname)
                          .getConstructor(classOf[FetcherConfigurationData]).newInstance(data).asInstanceOf[Fetcher[_<:ApplicationData]]
      _typeToFetcher += (data.getAppType -> instance)
      info(s"Load fetcher ${data.classname}")
    })
  }

  private def loadHeuristics={
    val heuristicsConfiguration = Utils.loadYmlDoc(Configs.HEURISTICS_CONF.getValue)(classOf[HeuristicConfiguration])

    heuristicsConfiguration.getHeuristics.forEach(data=>{
      val instance = Class.forName(data.classname)
                          .getConstructor(classOf[HeuristicConfigurationData]).newInstance(data).asInstanceOf[Heuristic]
      val value = _typeToHeuristics.getOrElseUpdate(data.getAppType, Nil)

      _typeToHeuristics.put(data.getAppType, value :+ instance)
      info(s"Load heuristic ${data.classname}")
    })
  }

  private def loadJobTypes={
    Utils.loadYmlDoc(Configs.JOBTYPES_CONF.getValue)(classOf[JobTypeConfiguration]).jobTypes
      .foldLeft( _appTypeToJobTypes )((m, jobTy)=>{
      if(m.contains(jobTy.getAppType))
        m(jobTy.getAppType) = m(jobTy.getAppType) :+ jobTy
      else
        m += (jobTy.getAppType -> List(jobTy))

      m
    })
  }

  private def loadGeneralConf={
    hadoopConf = new Configuration()
  }

  private def configureSupportedApplicationTypes(): Unit ={
    val supportedTypes = _typeToFetcher.keySet & _typeToHeuristics.keySet & _appTypeToJobTypes.keySet & _typeToAggregator.keySet

    _typeToFetcher.retain((t,_)=>{supportedTypes.contains(t)})
    _typeToHeuristics.retain((t,_)=>{supportedTypes.contains(t)})
    _appTypeToJobTypes.retain((t,_)=>{supportedTypes.contains(t)})
    _typeToAggregator.retain((t,_)=>{supportedTypes.contains(t)})
    supportedTypes.foldLeft(_nameToType)( (m,v)=>{m.put(v.upperName,v); m} )

    info("Configuring LannisterContext ... ")
    supportedTypes.foreach(tpe=>{
      info(
        s"""Supports ${tpe.upperName} application type, using ${_typeToFetcher(tpe).getClass} fetcher class with
           | Heuristics [ ${_typeToHeuristics(tpe).map(_.getClass).mkString(",")} ]  and following JobTypes
           | [ ${_appTypeToJobTypes(tpe).map(_.getClass).mkString(",")} ]
           |""".stripMargin )
    })
  }

}

