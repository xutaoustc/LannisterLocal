package com.lannister.dao

import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.lannister.model.AppResult
import org.springframework.stereotype.Repository


@Repository
trait AppResultDao extends BaseMapper[AppResult]{
    def upsert(appResult: AppResult): Int
}
