package com.ctyun.lannister

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class Lannister extends Runnable{
  @Autowired
  private var lannisterRunner: LannisterLogic = _

  override def run(): Unit = {
    lannisterRunner.run()
  }
}
