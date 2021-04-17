package com.ctyun.lannister

import org.mybatis.spring.annotation.MapperScan
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
@MapperScan(Array[String]("com.ctyun.lannister.dao"))
class Application

object Application {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[Application])
  }
}