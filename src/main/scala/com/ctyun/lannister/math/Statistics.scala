package com.ctyun.lannister.math

import java.lang.{Long => JLong}

object Statistics {

  val SECOND_IN_MS = 1000L

  def median(values:List[JLong]): Long ={
    val sorted = values.sorted
    val middle = sorted.size / 2
    if(sorted.size % 2 == 0)
      (sorted(middle - 1) + sorted(middle)) / 2
    else
      sorted(middle)
  }

  def percentile(values:List[JLong], percentile:Int): Long ={
    if(percentile == 0)
      return 0

    val sorted = values.sorted

    val position = Math.ceil(sorted.size * percentile / 100.0).toInt

    if(position == 0)
      return sorted(position)

    sorted(position-1)
  }

  def readableTimespan(milliseconds:Long):String={
    if (milliseconds == 0) {
      return "0 sec";
    }

    var seconds = milliseconds / 1000
    var minutes = seconds / 60
    val hours = minutes / 60
    minutes %= 60
    seconds %= 60

    var str = if (hours > 0)
                s"${hours} hr "
              else ""
    if (minutes > 0)
      str = s"${str}${minutes} min "
    if(seconds > 0)
      str = s"${str}${seconds} sec "
    str.trim
  }
}
