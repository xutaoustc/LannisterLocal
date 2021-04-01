package com.ctyun.lannister

import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.core.task.TaskExecutor


@Configuration
class BackgroundJobBoot {

  @Bean def taskExecutor = new SimpleAsyncTaskExecutor

  @Bean def schedulingRunner(taskExecutor: TaskExecutor): CommandLineRunner = new CommandLineRunner() {
    override def run(args: String*): Unit = {
      taskExecutor.execute(Lannister())
    }
  }
}
