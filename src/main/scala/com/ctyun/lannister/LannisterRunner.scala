package com.ctyun.lannister

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class LannisterRunner extends Runnable{
  @Autowired
  private var lannisterAnalyzer: LannisterAnalyzer = _

  override def run(): Unit = {
    lannisterAnalyzer.run()
  }
}
