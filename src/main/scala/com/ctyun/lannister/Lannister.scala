package com.ctyun.lannister

class Lannister private() extends Runnable{
  // TODO    println(Configs.AUTO_TUNING_ENABLED.getValue)

  private val lannister = new LannisterRunner

  override def run(): Unit = {
    // hear me roar!
    lannister.run()
  }
}

object Lannister{
  private val INSTANCE = new Lannister

  def apply()={
    INSTANCE
  }
}