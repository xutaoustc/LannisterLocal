package com.ctyun.lannister.dao

import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.ctyun.lannister.model.AppHeuristicResult
import org.springframework.stereotype.Repository

/**
  * @author anlexander
  * @date 2021/4/14
  */
@Repository
trait AppHeuristicResultDao extends BaseMapper[AppHeuristicResult]{
     def upsert(appHeuristicResult:AppHeuristicResult):Int
}
