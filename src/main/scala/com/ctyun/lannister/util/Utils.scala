package com.ctyun.lannister.util

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object Utils {
  def getJvmUser:String = System.getProperty("user.name")

  def loadYmlDoc[T](path:String)(clazz:Class[T]):T={
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    val yaml = new Yaml(new Constructor(clazz))
    yaml.load(stream).asInstanceOf[T]
  }
}

