package com.ctyun.lannister

import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.core.task.TaskExecutor


@Configuration
class Booter {

  @Bean def taskExecutor = new SimpleAsyncTaskExecutor

  @Bean def schedulingRunner(executor: TaskExecutor, lannister: Lannister): CommandLineRunner = new CommandLineRunner() {
    override def run(args: String*): Unit = {
      executor.execute(lannister)
    }
  }
}
