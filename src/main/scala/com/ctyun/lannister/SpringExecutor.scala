package com.ctyun.lannister

import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor

@Configuration
class SpringExecutor {
  @Bean(name = Array("executor"))
  def taskExecutor: TaskExecutor = new SimpleAsyncTaskExecutor

  @Bean
  def runner(executor: TaskExecutor, lannisterRunner: LannisterRunner): CommandLineRunner =
    new CommandLineRunner() {
      override def run(args: String*): Unit = {
        executor.execute(lannisterRunner)
      }
    }
}
