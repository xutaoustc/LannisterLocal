package com.ctyun.lannister.conf.jobtype

import com.ctyun.lannister.analysis.JobType
import java.util
import scala.beans.BeanProperty


class JobTypeConfiguration {
  @BeanProperty var jobTypes: util.ArrayList[JobType] = _
}
