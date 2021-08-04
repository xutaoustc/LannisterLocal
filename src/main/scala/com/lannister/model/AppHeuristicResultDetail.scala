package com.lannister.model

import com.baomidou.mybatisplus.annotation.TableName

@TableName("app_heuristic_result_details")
class AppHeuristicResultDetail extends AppBase {
  var name: String = _
  var value: String = _
  var resultId: Long = _
  var heuristicId: Long = _
}


object AppHeuristicResultDetail {
  def apply(name: String, value: String): AppHeuristicResultDetail = {
    val heuDetailSave = new AppHeuristicResultDetail
    heuDetailSave.name = name
    heuDetailSave.value = value
    heuDetailSave
  }
}