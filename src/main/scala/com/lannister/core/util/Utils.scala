package com.lannister.core.util

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object Utils {
  def getJvmUser : String = System.getProperty("user.name")

  def loadYml[T](path: String)(clazz: Class[T]): T = {
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    val yaml = new Yaml(new Constructor(clazz))
    yaml.load(stream).asInstanceOf[T]
  }

  def executeWithRetTime[T](f : => T): (Long, T) = {
    val analysisStartTimeMillis = System.currentTimeMillis
    val res: T = f
    (System.currentTimeMillis() - analysisStartTimeMillis, res)
  }

  def getOrElse[U, V](v : U, f : U => V, d : V): V = {
    if (v == null) {
      d
    } else {
      f(v)
    }
  }

  def failureRate(pri: Double, other: Double*): Option[Double] = {
    val sum = pri + other.sum
    if (sum == 0) None else Some(pri / sum)
  }

  def tryFinally[U](action: => U)(fiFun: => Unit): U = {
    try {
      action
    } finally {
      fiFun
    }
  }

  // scalastyle:off classforname
  def classForName[C]( className: String,
                       initialize: Boolean = true): Class[C] = {
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
      asInstanceOf[Class[C]]
    // scalastyle:on classforname
  }

  import org.json4s._
  import org.json4s.native.Serialization
  import org.json4s.native.Serialization._
  implicit val formats = Serialization.formats(NoTypeHints)
  def toJson(obj: Any): String = {
    write(obj)
  }
}

