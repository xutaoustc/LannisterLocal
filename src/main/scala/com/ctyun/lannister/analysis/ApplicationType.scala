package com.ctyun.lannister.analysis

import java.util.Locale

case class ApplicationType(private val name : String) {
  def upperName: String = name.toUpperCase(Locale.ROOT)
}
