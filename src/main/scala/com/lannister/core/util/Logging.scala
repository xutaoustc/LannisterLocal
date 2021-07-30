package com.lannister.core.util

import org.slf4j.LoggerFactory

trait Logging {

  protected lazy implicit val logger = LoggerFactory.getLogger(getClass)

  def trace(message: => String): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(message)
    }
  }

  def debug(message: => String): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message)
    }
  }

  def info(message: => String): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message)
    }
  }

  def warn(message: => String): Unit = {
    logger.warn(message)
  }

  def error(message: => String): Unit = {
    logger.error(message)
  }

  def info(message: => String, t: Throwable): Unit = {
    logger.info(message, t)
  }

  def warn(message: => String, t: Throwable): Unit = {
    logger.warn(message, t)
  }

  def error(message: => String, t: Throwable): Unit = {
    logger.error(message, t)
  }

}
