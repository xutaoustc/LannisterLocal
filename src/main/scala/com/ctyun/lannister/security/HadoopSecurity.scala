package com.ctyun.lannister.security

import java.io.File

import com.ctyun.lannister.conf.Configs._
import com.ctyun.lannister.hadoop.HadoopConf
import com.ctyun.lannister.util.{Logging, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

class HadoopSecurity extends Logging{
  private var loginUser: UserGroupInformation = _

  UserGroupInformation.setConfiguration(HadoopConf.conf)
  if (UserGroupInformation.isSecurityEnabled) {
    info("This cluster is kerberos enabled")
    check()
  }

  def getUGI: UserGroupInformation = {
    checkLogin()
    loginUser
  }

  private def checkLogin(): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      if (loginUser == null) {
        UserGroupInformation.loginUserFromKeytab(KEYTAB_USER.getValue, KEYTAB_LOCATION.getValue)
        loginUser = UserGroupInformation.getLoginUser
      } else {
        loginUser.checkTGTAndReloginFromKeytab
      }
    } else {
      loginUser = UserGroupInformation.createRemoteUser(Utils.getJvmUser)
    }
  }

  private def check() = {
    if (StringUtils.isBlank(KEYTAB_USER.getValue)) {
      throw new IllegalArgumentException("kerberos user not set")
    }

    if (StringUtils.isBlank(KEYTAB_LOCATION.getValue)) {
      throw new IllegalArgumentException("kerberos location not set")
    } else if (! new File(KEYTAB_LOCATION.getValue).exists() ) {
      throw new IllegalArgumentException(
        s"The keytab location ${KEYTAB_LOCATION.getValue} does not exist")
    }
  }
}


object HadoopSecurity{
  private val INSTANCE = new HadoopSecurity

  def apply(): HadoopSecurity = {
    INSTANCE
  }
}
