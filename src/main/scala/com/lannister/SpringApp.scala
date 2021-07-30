package com.lannister

import org.mybatis.spring.annotation.MapperScan
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
@MapperScan(Array[String]("com.lannister.dao"))
class SpringApp


object SpringApp {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[SpringApp])
  }
}
