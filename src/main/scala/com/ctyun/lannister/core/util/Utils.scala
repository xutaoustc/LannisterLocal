package com.ctyun.lannister.core.util

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

  // scalastyle:off classforname
  def classForName[C]( className: String,
                       initialize: Boolean = true): Class[C] = {
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
      asInstanceOf[Class[C]]
    // scalastyle:on classforname
  }
}

