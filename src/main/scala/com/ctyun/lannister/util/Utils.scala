package com.ctyun.lannister.util

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import java.io.FileInputStream

object Utils {
  def getJvmUser:String = System.getProperty("user.name")

  def loadYmlDoc[T](path:String)(clazz:Class[T]):T={
    val url = getClass.getClassLoader.getResource(path)
    val inputStream = new FileInputStream(url.getPath)
    val yaml = new Yaml(new Constructor(clazz))
    yaml.load(inputStream).asInstanceOf[T]
  }
}

