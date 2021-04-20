package com.ctyun.lannister.controller

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.ctyun.lannister.dao.{AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.model.{AppBase, AppResult}
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


  def selectId[T <: AppBase](dao:BaseMapper[T],entity:T, column:String,value:Any):Long = {
    if(entity.id != 0){
      entity.id
    }else{
      val wrapper = new QueryWrapper[T]().eq(column,value)
      dao.selectOne(wrapper).id
    }
  }


  @RequestMapping(value=Array("/insert"), method=Array(RequestMethod.POST))
  def insert()={
        val appResult = new AppResult()
        appResult.appId = "application_4"
        appResult.trackingUrl = "www.cctv.com"
        appResult.queueName = "dhfksdhfgkshgk"
        appResult.username = "lisi"
        appResult.startTime = 1l
        appResult.finishTime = 2l
        appResult.name = "zhangsan1"
        appResult.jobType = "sparksql"
        appResult.resourceUsed = 100l
        appResult.totalDelay = 200l
        appResult.resourceWasted = 100l
        appResult.severityId = 0
        appResult.score = 100
        //val res = appResultDao.insert(appResult)
        //println(s"当前添加结果为：$res")
         appResultDao.upsert(appResult)
         val res = selectId[AppResult](appResultDao,appResult, "app_id",appResult.appId)
         println("当前添加结果为: " + appResult.id  + "," + res)

  }
}
