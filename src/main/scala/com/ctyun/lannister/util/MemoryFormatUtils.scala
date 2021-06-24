package com.ctyun.lannister.util

import java.text.DecimalFormat
import java.util.Locale
import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils

object MemoryFormatUtils {

  private class MemoryUnit(val _name: String, val _bytes: Long) {
    def getName: String = _name

    def getBytes: Long = _bytes

    override def toString: String = _name
  }

  // Units must be in a descent order
  private val UNITS = Array[MemoryFormatUtils.MemoryUnit](
    new MemoryFormatUtils.MemoryUnit("TB", 1L << 40),
    new MemoryFormatUtils.MemoryUnit("GB", 1L << 30),
    new MemoryFormatUtils.MemoryUnit("MB", 1L << 20),
    new MemoryFormatUtils.MemoryUnit("KB", 1L << 10),
    new MemoryFormatUtils.MemoryUnit("B", 1L))

  private val FORMATTER = new DecimalFormat("#,##0.##")
  private val REGEX_MATCHER = Pattern.compile(
    "([-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?)\\s*((?:[T|G|M|K])?B?)?", Pattern.CASE_INSENSITIVE)


  def bytesToString(value: Long): String = {
    if (value < 0) {
      return value.toString
    }

    UNITS.foreach(u => {
      val bytes = u.getBytes
      if (value >= bytes) {
        val numResult = if (bytes > 1) {
          value.toDouble / bytes.toDouble
        } else {
          value.toDouble
        }
        return FORMATTER.format(numResult) + " " + u.getName
      }
    })

    value + " " + UNITS(UNITS.length - 1).getName
  }

  /**
   * Convert a formatted string into a long value in bytes.
   * This method handles
   *
   * @param formattedString The string to convert
   * @return The bytes value
   */
  def stringToBytes(formattedString: String): Long = {
    if (formattedString == null) {
      return 0L
    }

    val matcher = REGEX_MATCHER.matcher(formattedString.replace(",", ""))
    if (!matcher.matches) {
      throw new IllegalArgumentException(
        s"The formatted string [$formattedString] does not match with " +
          s"the regex /$REGEX_MATCHER.toString/")
    }
    if (matcher.groupCount != 1 && matcher.groupCount != 2) throw new IllegalArgumentException
    val numPart = matcher.group(1).toDouble
    if (numPart < 0) {
      throw new IllegalArgumentException(
        "The number part of the memory cannot be less than zero: [" + numPart + "].")
    }
    var unitPart = if (matcher.groupCount == 2) {
      matcher.group(2).toUpperCase(Locale.ROOT)
    } else ""
    if (!unitPart.endsWith("B")) unitPart += "B"

    UNITS.foreach(u => {
      if (unitPart == u.getName) {
        return (numPart * u.getBytes).toLong
      }
    })

    throw new IllegalArgumentException(
      "The formatted string [" + formattedString + "] 's unit part [" + unitPart + "] " +
      "does not match any unit. The supported units are (case-insensitive, " +
      "and also the 'B' is ignorable): [" + StringUtils.join(UNITS) + "].")
  }
}

