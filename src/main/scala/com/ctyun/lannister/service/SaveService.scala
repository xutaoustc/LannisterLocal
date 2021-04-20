package com.ctyun.lannister.service

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.ctyun.lannister.dao.{AppHeuristicResultDao, AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.model.{AppBase, AppHeuristicResult, AppHeuristicResultDetails, AppResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class SaveService {
  @Autowired
  var appResultDao: AppResultDao = _
  @Autowired
  var appHeuristicResultDao: AppHeuristicResultDao = _
  @Autowired
  var appHeuristicResultDetailsDao: AppHeuristicResultDetailsDao = _

  @Transactional
  def save(result:AppResult)={
    appResultDao.upsert(result)
    val appResultId = readId[AppResult](appResultDao,result, "app_id",result.appId)
    result.heuristicResults.foreach(heuResult=>{
      heuResult.resultId = appResultId
      appHeuristicResultDao.upsert(heuResult)
      val heuResultId = readId[AppHeuristicResult](appHeuristicResultDao,heuResult, "result_id",heuResult.resultId)
      heuResult.heuristicResultDetails.foreach{ heuResultDetail =>{
        heuResultDetail.heuristicId = heuResultId
        appHeuristicResultDetailsDao.upsert(heuResultDetail)
      }}
    })
  }

  def readId[T <: AppBase](dao:BaseMapper[T],entity:T, column:String,value:Any):Long = {
    if(entity.id != 0)
      entity.id
    else
      dao.selectOne(new QueryWrapper[T]().eq(column,value)).id
  }

}
