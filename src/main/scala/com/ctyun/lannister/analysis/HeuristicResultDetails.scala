package com.ctyun.lannister.analysis

case class HeuristicResultDetails(name:String, value:String, details:String){
  def this(name:String, value:String){
    this(name,value, null)
  }
}
