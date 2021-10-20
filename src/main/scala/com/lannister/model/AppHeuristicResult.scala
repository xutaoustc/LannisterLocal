package com.lannister.model

import com.baomidou.mybatisplus.annotation.TableName
import com.lannister.core.domain.HeuristicResult
import com.lannister.core.util.Utils


@TableName("app_heuristic_result")
class AppHeuristicResult extends AppBase {
  var heuristicClass: String = _
  var heuristicName: String = _
  var heuristicSeverityId: Int = _
  var score: Int = _
  var details: String = _
  var resultId: Long = _
}

object AppHeuristicResult{
  implicit def hr2AppHr(hr : HeuristicResult) : AppHeuristicResult = {
    val appHR = new AppHeuristicResult
    appHR.heuristicClass = hr.heuristicClass
    appHR.heuristicName = hr.heuristicName
    appHR.heuristicSeverityId = hr.heuristicSeverity.id
    appHR.score = hr.score
    appHR.details = Utils.toJson(hr.hds.map(hd => hd.name -> hd.value).toMap)
    appHR
  }
}
