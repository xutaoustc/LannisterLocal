package com.lannister.service

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.lannister.dao.{AppHeuristicResultDao, AppHeuristicResultDetailDao, AppResultDao}
import com.lannister.model.{AppBase, AppResult}
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
  private var appHeuristicResultDetailDao: AppHeuristicResultDetailDao = _

  @Transactional(isolation = Isolation.READ_COMMITTED)
  def save(result: AppResult): Unit = {
    appResultDao.upsert(result)
    val resultId = readId[AppResult](result, "app_id", result.appId)
    appHeuristicResultDao.delete( new QueryWrapper().eq("result_id", resultId) )
    appHeuristicResultDetailDao.delete(new QueryWrapper().eq("result_id", resultId) )

    result.appHRs.foreach(appHR => {
      appHR.resultId = resultId
      appHeuristicResultDao.insert(appHR)

      appHR.appHDs.foreach{ appHD => {
        appHD.resultId = resultId
        appHD.heuristicId = appHR.id
        appHeuristicResultDetailDao.insert(appHD)
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
