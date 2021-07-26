package com.ctyun.lannister.util

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object Utils {
  def getJvmUser : String = System.getProperty("user.name")

  def loadYml[T](path: String)(clazz: Class[T]): T = {
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    val yaml = new Yaml(new Constructor(clazz))
    yaml.load(stream).asInstanceOf[T]
  }

  // scalastyle:off classforname
  def classForName[C](
                       className: String,
                       initialize: Boolean = true,
                       noSparkClassLoader: Boolean = false): Class[C] = {
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
      asInstanceOf[Class[C]]
    // scalastyle:on classforname
  }
}

