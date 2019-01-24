import java.io.FileInputStream
import java.util
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import org.apache.commons.io.FilenameUtils

object main {

  def main(args:Array[String]): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val conf = loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    val downloader = new DownloadFile()

    val fileToDownload = getFile2Download()

    val pathTo = "src/main/resources/"

    downloader.downloadFile(fileToDownload,"src/main/resources/" + fileToDownload)



  }

  def loadSparkProps(propFile: String): SparkConf ={

    val sparkConfigFile = propFile

    val sparkProp = new Properties()
    sparkProp.load(new FileInputStream(sparkConfigFile))

    sparkProp.getProperty("")
    val conf = new SparkConf()

    val keys = sparkProp.keys()

    while(keys.hasMoreElements){

      val key = String.valueOf(keys.nextElement())

      conf.set(key,sparkProp.getProperty(key))

    }

    conf
  }

  def getFile2Download() = {
      "2018-03-01-0.json.gz"
  }

}
