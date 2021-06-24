package com.ctyun.lannister

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class Lannister extends Runnable{
  // TODO    println(Configs.AUTO_TUNING_ENABLED.getValue)

  @Autowired
  private var lannisterRunner: LannisterRunner = _

  override def run(): Unit = {
    // hear me roar!
    lannisterRunner.run()
  }
}
