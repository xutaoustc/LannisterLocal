package com.ctyun.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableId}

/**
  * @author anlexander
  * @date 2021/4/19
  */
class AppBase {
  @TableId(`type` = IdType.AUTO)
  var id:Long = _
}
