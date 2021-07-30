package com.lannister.controller

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.lannister.dao.{AppHeuristicResultDetailDao, AppResultDao}
import com.lannister.model.{AppBase, AppResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/test"))
class Test {
  @Autowired
  var appResultDao: AppResultDao = _
  @Autowired
  var appHeuristicResultDetailDao: AppHeuristicResultDetailDao = _

  @RequestMapping(value = Array("/aa"), method = Array(RequestMethod.GET))
  def aa(): String = {
    "s"
  }


  def selectId[T <: AppBase](dao: BaseMapper[T], entity: T, column: String, value: Any): Long = {
    if (entity.id != 0) {
      entity.id
    } else {
      val wrapper = new QueryWrapper[T]().eq(column, value)
      dao.selectOne(wrapper).id
    }
  }


  @RequestMapping(value = Array("/insert"), method = Array(RequestMethod.POST))
  def insert(): Unit = {
    val appResult = new AppResult()
    appResult.appId = "application_4"
    appResult.trackingUrl = "www.cctv.com"
    appResult.queueName = "dhfksdhfgkshgk"
    appResult.username = "lisi"
    appResult.startTime = 1L
    appResult.finishTime = 2L
    appResult.name = "zhangsan1"
    appResult.jobType = "sparksql"
    appResult.resourceUsed = 100L
    appResult.totalDelay = 200L
    appResult.resourceWasted = 100L
    appResult.severityId = 0
    appResult.score = 100
    appResultDao.upsert(appResult)
    val res = selectId[AppResult](appResultDao, appResult, "app_id", appResult.appId)
  }
}
