package com.lannister.core.conf

import java.io.{File, FileInputStream, InputStream, IOException}
import java.util.Properties

import scala.collection.JavaConverters._

import com.lannister.core.util.Logging
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils


private[conf] object ConfigurationUtil extends Logging {

  val DEFAULT_PROPERTY_FILE_NAME = "lannister.properties"

  private val config = new Properties
  private val sysProps = sys.props
  private val env = sys.env

  try {
    init
  } catch {
    case e: Throwable =>
      warn("Failed to init conf", e)
  }

  private def init: Unit = {
    val propertyFile = sysProps.getOrElse("lannister.configuration", DEFAULT_PROPERTY_FILE_NAME)
    val configFileURL = getClass.getClassLoader.getResource(propertyFile)
    if (configFileURL != null && new File(configFileURL.getPath).exists) {
      initConfig(config, configFileURL.getPath)
    } else warn(s"****** Notice: The lannister config file $propertyFile is not exists! ******")
  }

  private def initConfig(config: Properties, filePath: String) {
    var inputStream: InputStream = null
    try {
      inputStream = new FileInputStream(filePath)
      config.load(inputStream)
    } catch {
      case e: IOException =>
        error("Can't load " + filePath, e)
    } finally IOUtils.closeQuietly(inputStream)
  }

  def getOption(key: String): Option[String] = {
    val value = config.getProperty(key)
    if(StringUtils.isNotEmpty(value)) {
      return Some(value)
    }
    val propsValue = sysProps.get(key).orElse(sys.props.get(key))
    if(propsValue.isDefined) {
      return propsValue
    }
    env.get(key)
  }

  def getOption[T](commonVars: CommonVars[T]): Option[T] = if (commonVars.value != null) {
    Option(commonVars.value)
  } else {
    val value = ConfigurationUtil.getOption(commonVars.key)
    if (value.isEmpty) Option(commonVars.defaultValue)
    else formatValue(commonVars.defaultValue, value)
  }

  private[conf] def formatValue[T](defaultValue: T, value: Option[String]): Option[T] = {
    if (value.isEmpty || value.exists(StringUtils.isEmpty)) return Option(defaultValue)
    val formattedValue = defaultValue match {
      case _: String => value
      case _: Byte => value.map(_.toByte)
      case _: Short => value.map(_.toShort)
      case _: Char => value.map(_.toCharArray.apply(0))
      case _: Int => value.map(_.toInt)
      case _: Long => value.map(_.toLong)
      case _: Float => value.map(_.toFloat)
      case _: Double => value.map(_.toDouble)
      case _: Boolean => value.map(_.toBoolean)
      case null => value
    }
    formattedValue.asInstanceOf[Option[T]]
  }



  def getBoolean(key: String, default: Boolean): Boolean =
    getOption(key).map(_.toBoolean).getOrElse(default)
  def getBoolean(commonVars: CommonVars[Boolean]): Option[Boolean] = getOption(commonVars)

  def get(key: String, default: String): String = getOption(key).getOrElse(default)
  def get(commonVars: CommonVars[String]): Option[String] = getOption(commonVars)

  def get(key: String): String = getOption(key).getOrElse(throw new NoSuchElementException(key))

  def getInt(key: String, default: Int): Int = getOption(key).map(_.toInt).getOrElse(default)
  def getInt(commonVars: CommonVars[Int]): Option[Int] = getOption(commonVars)

  def contains(key: String): Boolean = getOption(key).isDefined


  def properties: Properties = {
    val props = new Properties
    props.putAll(sysProps.asJava)
    props.putAll(config)
    props.putAll(env.asJava)
    props
  }
}
