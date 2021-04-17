package com.ctyun.lannister.controller

import com.ctyun.lannister.dao.{AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.model.AppResult
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/test"))
class Test {
  @Autowired
  var appResultDao: AppResultDao = _
  @Autowired
  var appHeuristicResultDetailsDao: AppHeuristicResultDetailsDao = _

  @RequestMapping(value=Array("/aa"), method=Array(RequestMethod.GET))
  def aa()={
    "s"
  }

  @RequestMapping(value=Array("/insert"), method=Array(RequestMethod.POST))
  def insert()={
        val appResult = new AppResult()
        appResult.appId = "application_1"
        appResult.trackingUrl = "www.cctv.com"
        appResult.queueName = "default"
        appResult.username = "zhangsan"
        appResult.startTime = 1l
        appResult.finishTime = 2l
        appResult.name = "zhangsan1"
        appResult.jobType = "sparksql"
        appResult.resourceUsed = 100l
        appResult.totalDelay = 200l
        appResult.resourceWasted = 100l
        appResult.severityId = 0
        appResult.score = 100
        val res = appResultDao.insert(appResult)
        println(s"当前添加结果为：$res")

  }
}
