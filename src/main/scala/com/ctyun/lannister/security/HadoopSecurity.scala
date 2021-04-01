package com.ctyun.lannister.security

import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.util.{Logging, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import java.io.File

class HadoopSecurity extends Logging{
  private val conf = new Configuration()
  private var loginUser:UserGroupInformation = _

  UserGroupInformation.setConfiguration(conf)
  if(UserGroupInformation.isSecurityEnabled){
    info("This cluster is kerberos enabled")
    check()
  }

  def getUGI ={
    checkLogin()
    loginUser
  }

  private def checkLogin()={
    if(!UserGroupInformation.isSecurityEnabled) {
      if(loginUser == null){
        UserGroupInformation.loginUserFromKeytab(Configs.KEYTAB_USER.getValue, Configs.KEYTAB_LOCATION.getValue)
        loginUser = UserGroupInformation.getLoginUser
      }else{
        loginUser.checkTGTAndReloginFromKeytab
      }
    } else{
      loginUser = UserGroupInformation.createRemoteUser(Utils.getJvmUser)
    }
  }

  private def check()={
    if(StringUtils.isBlank(Configs.KEYTAB_USER.getValue)){
      throw new IllegalArgumentException("kerberos user not set")
    }

    if(StringUtils.isBlank(Configs.KEYTAB_LOCATION.getValue)){
      throw new IllegalArgumentException("kerberos location not set")
    }else if( new File(Configs.KEYTAB_LOCATION.getValue).exists() ){
      throw new IllegalArgumentException(s"The keytab location ${Configs.KEYTAB_LOCATION.getValue} does not exist")
    }
  }
}


object HadoopSecurity{
  private val INSTANCE = new HadoopSecurity

  def apply()={
    INSTANCE
  }
}