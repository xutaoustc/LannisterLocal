package com.ctyun.lannister.dao

import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.ctyun.lannister.model.AppHeuristicResultDetails
import org.springframework.stereotype.Repository

/**
  * @author anlexander
  * @date 2021/4/14
  */
@Repository
trait AppHeuristicResultDetailsDao extends BaseMapper[AppHeuristicResultDetails]{
  def upsert(appHeuristicResultDetails:AppHeuristicResultDetails):Int
}
