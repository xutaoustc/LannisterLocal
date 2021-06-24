package com.ctyun.lannister.conf

import java.util.Properties

import scala.collection.JavaConverters._

case class CommonVars[T](key: String, defaultValue: T, private[conf] val value: T,
                         description: String = null) {
  val getValue: T = ConfigurationUtil.getOption(this).getOrElse(defaultValue)
  def getValue(properties: java.util.Map[String, String]): T = {
    if (properties == null || !properties.containsKey(key) || properties.get(key) == null) getValue
    else ConfigurationUtil.formatValue(defaultValue, Option(properties.get(key))).get
  }
  def getValue(properties: Map[String, String]): T = getValue(mapAsJavaMap(properties))
}
object CommonVars {
  def apply[T](key: String, defaultValue: T, description: String): CommonVars[T] =
    CommonVars(key, defaultValue, null.asInstanceOf[T], description)

  implicit def apply[T](key: String, defaultValue: T): CommonVars[T] =
    new CommonVars(key, defaultValue, null.asInstanceOf[T], null)

  implicit def apply[T](key: String): CommonVars[T] = apply(key, null.asInstanceOf[T])

  def properties: Properties = ConfigurationUtil.properties

}
