package com.ctyun.lannister

class Lannister private() extends Runnable{
  // TODO    println(Configs.AUTO_TUNING_ENABLED.getValue)

  private val lannisterRunner = new LannisterRunner

  override def run(): Unit = {
    lannisterRunner.run()
  }
}

object Lannister{
  private val INSTANCE:Lannister = new Lannister

  def apply():Lannister={
    INSTANCE
  }
}