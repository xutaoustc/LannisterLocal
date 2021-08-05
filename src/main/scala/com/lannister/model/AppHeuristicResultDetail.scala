package com.lannister.model

import com.baomidou.mybatisplus.annotation.TableName
import com.lannister.core.domain.{HeuristicResultDetail => HD}

@TableName("app_heuristic_result_details")
class AppHeuristicResultDetail extends AppBase {
  var name: String = _
  var value: String = _
  var resultId: Long = _
  var heuristicId: Long = _
}


object AppHeuristicResultDetail {
  implicit def hd2AppHd(hd : HD) : AppHeuristicResultDetail = {
    val appHd = new AppHeuristicResultDetail
    appHd.name = hd.name
    appHd.value = hd.value
    appHd
  }

  implicit def hdList2AppHdList(hdList: List[HD]) : List[AppHeuristicResultDetail] = {
    hdList.map(hd2AppHd)
  }
}
