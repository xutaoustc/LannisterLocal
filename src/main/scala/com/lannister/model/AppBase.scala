package com.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableId}

class AppBase {
  @TableId(`type` = IdType.AUTO)
  var id: Long = _
}
