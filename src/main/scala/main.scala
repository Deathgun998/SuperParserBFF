import java.io.FileInputStream
import java.util
import java.util.Properties

import file.DownloadFile
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.io.FilenameUtils
import sparkconfig.SparkConfig

object main {

  def main(args:Array[String]): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    val downloader = new DownloadFile()

    val downloadURL = getUrl();

    val fileToDownload = getFile2Download()

    val pathTo = "src/main/resources/"

    downloader.downloadFile(downloadURL + fileToDownload,pathTo + fileToDownload)

    downloader.decompressGZ(pathTo + fileToDownload,pathTo + fileToDownload.substring(0, fileToDownload.lastIndexOf('.')))

    println("File gz cancellato: " + downloader.deleteFile(pathTo+fileToDownload))

  }

  def getFile2Download() : String = {
     return "2018-03-01-0.json.gz";
  }
  def getUrl(): String ={
    return "http://data.githubarchive.org/";
  }

}
