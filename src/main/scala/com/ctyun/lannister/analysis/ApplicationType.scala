package com.ctyun.lannister.analysis

case class ApplicationType(private val name:String){
  def upperName = name.toUpperCase
}
