package com.ctyun.lannister.service

import com.ctyun.lannister.dao.{AppHeuristicResultDao, AppHeuristicResultDetailsDao, AppResultDao}
import com.ctyun.lannister.model.AppResult
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
    appResultDao.insert(result)
    result.heuristicResults.foreach(heuResult=>{
      appHeuristicResultDao.insert(heuResult)
      heuResult.heuristicResultDetails.foreach(appHeuristicResultDetailsDao.insert(_))
    })
  }
}
