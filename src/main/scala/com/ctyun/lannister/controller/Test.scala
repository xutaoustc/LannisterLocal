package com.ctyun.lannister.controller

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/test"))
class Test {

  @RequestMapping(value=Array("/aa"), method=Array(RequestMethod.GET))
  def aa()={
    "s"
  }
}
