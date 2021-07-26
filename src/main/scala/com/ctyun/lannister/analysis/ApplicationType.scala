package com.ctyun.lannister.analysis

import java.util.Locale

case class ApplicationType(private val _name : String) {
  def name: String = _name.toUpperCase(Locale.ROOT)
}
