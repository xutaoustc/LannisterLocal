package com.ctyun.lannister.service

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.ctyun.lannister.dao.{AppHeuristicResultDao, AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.model.{AppBase, AppHeuristicResult, AppHeuristicResultDetails, AppResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.{Isolation, Transactional}

@Component
class PersistService {
  @Autowired
  private var appResultDao: AppResultDao = _
  @Autowired
  private var appHeuristicResultDao: AppHeuristicResultDao = _
  @Autowired
  private var appHeuristicResultDetailsDao: AppHeuristicResultDetailsDao = _

  @Transactional(isolation = Isolation.READ_COMMITTED)
  def save(result: AppResult): Unit = {
    appResultDao.upsert(result)
    val resultId = readId[AppResult](result, "app_id", result.appId)
    appHeuristicResultDao.delete( new QueryWrapper[AppHeuristicResult]().eq("result_id", resultId) )
    appHeuristicResultDetailsDao.delete(
      new QueryWrapper[AppHeuristicResultDetails]().eq("result_id", resultId) )

    result.heuristicResults.foreach(heuResult => {
      heuResult.resultId = resultId
      appHeuristicResultDao.insert(heuResult)
      heuResult.heuristicResultDetails.foreach{ heuResultDetail => {
        heuResultDetail.resultId = resultId
        heuResultDetail.heuristicId = heuResult.id
        appHeuristicResultDetailsDao.insert(heuResultDetail)
      }}
    })
  }

  private def readId[T <: AppBase](entity: T, column: String, value: Any): Long = {
    if (entity.id != 0) {
      entity.id
    } else {
      appResultDao.selectOne(new QueryWrapper().eq(column, value)).id
    }
  }

}
