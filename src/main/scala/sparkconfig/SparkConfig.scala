package sparkconfig

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkConf

class SparkConfig {

  def loadSparkProps(propFile: String): SparkConf ={

    val sparkConfigFile = propFile

    val sparkProp = new Properties()
    sparkProp.load(new FileInputStream(sparkConfigFile))

    val conf = new SparkConf()

    val keys = sparkProp.keys()

    while(keys.hasMoreElements){

      val key = String.valueOf(keys.nextElement())

      conf.set(key,sparkProp.getProperty(key))

    }

    conf
  }

}
