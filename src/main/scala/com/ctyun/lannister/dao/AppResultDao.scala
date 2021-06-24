package com.ctyun.lannister.dao

import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.ctyun.lannister.model.AppResult
import org.springframework.stereotype.Repository


@Repository
trait AppResultDao extends BaseMapper[AppResult]{
    def upsert(appResult: AppResult): Int
}
