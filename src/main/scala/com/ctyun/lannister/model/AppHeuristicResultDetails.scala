package com.ctyun.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableId, TableName}

@TableName("app_heuristic_result_details")
class AppHeuristicResultDetails extends AppBase {
  var name:String = _
  var value:String = _
  var details:String = _
  var heuristicId:Long = _
}
