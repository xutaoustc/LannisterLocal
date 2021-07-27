package com.ctyun.lannister.core.hadoop

import java.io.File
import java.security.PrivilegedAction

import com.ctyun.lannister.core.conf.Configs._
import com.ctyun.lannister.core.util.{Logging, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation


class HadoopSecurity extends Logging{
  UserGroupInformation.setConfiguration(HadoopConf.conf)
  if (UserGroupInformation.isSecurityEnabled) {
    info("This cluster is kerberos enabled")
    check()
  }

  private def check() = {
    if (StringUtils.isBlank(KEYTAB_USER.getValue)) {
      throw new IllegalArgumentException("kerberos user not set")
    }

    if (StringUtils.isBlank(KEYTAB_LOCATION.getValue)) {
      throw new IllegalArgumentException("kerberos location not set")
    } else if (! new File(KEYTAB_LOCATION.getValue).exists() ) {
      throw new IllegalArgumentException(s"Keytab loc ${KEYTAB_LOCATION.getValue} does not exist")
    }
  }


  private var loginUser: UserGroupInformation = _

  def doAs[T](f: () => T): T = {
    getUGI.doAs(
      new PrivilegedAction[T]() {
        override def run(): T = f()
      }
    )
  }

  private def getUGI: UserGroupInformation = {
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
}


object HadoopSecurity{
  private val INSTANCE = new HadoopSecurity

  def apply(): HadoopSecurity = {
    INSTANCE
  }
}
