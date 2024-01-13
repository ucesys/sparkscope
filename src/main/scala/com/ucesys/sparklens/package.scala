package com.ucesys

import org.apache.spark.SparkConf

package object sparklens {

  def getDumpDirectory(conf: SparkConf): String = {
    conf.get("spark.sparklens.data.dir", "/tmp/sparklens/")
  }

  def asyncReportingEnabled(conf: SparkConf): Boolean = {
    // This will dump info to `getDumpDirectory()` and not run reporting
    conf.getBoolean("spark.sparklens.reporting.disabled", false)
  }

  def dumpDataEnabled(conf: SparkConf): Boolean = {
    /* Even if reporting is in app, we can still dump sparklens data which could be used later */
    conf.getBoolean("spark.sparklens.save.data", true)
  }
}
