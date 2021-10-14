package com.lannister.controller

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
@RequestMapping(Array("/debug"))
class DebugController {

  @RequestMapping(value = Array("/run"), method = Array(RequestMethod.POST))
  def run(appID: String): String = {
    "s"
  }

}
