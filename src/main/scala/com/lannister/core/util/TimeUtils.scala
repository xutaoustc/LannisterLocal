package com.lannister.core.util

object TimeUtils {
  val SECOND_IN_MS = 1000L

  def timeFormat(milliseconds: Long): String = {
    if (milliseconds == 0) {
      return "0 sec"
    }

    var seconds = milliseconds / 1000
    var minutes = seconds / 60
    val hours = minutes / 60
    minutes %= 60
    seconds %= 60

    var str = if (hours > 0) {
      s"$hours hr "
    } else ""
    if (minutes > 0) {
      str = s"$str$minutes min "
    }
    if (seconds > 0) {
      str = s"$str$seconds sec "
    }
    str.trim
  }
}
