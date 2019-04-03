import java.io.FileInputStream
import java.sql.Connection
import java.util
import java.util.Properties

import classes.{Actor, JsonRow, Payload, Repo}
import config.application.ApplicationConfig
import file.DownloadFile
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import config.spark.SparkConfig
import counters.rdd.{JSONFinderRDD, JSONMaxMinRDD}
import dao.DBConnector

object main {

  def main(args:Array[String]): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //SPARK CONGIGURATION

    System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.0")

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    //DOWNLOAD FILE

    /*val applicationConf = new ApplicationConfig("src/main/resources/application.properties")

    val downloadURL = applicationConf.getFileUrl()

    val fileToDownload = applicationConf.getFile2Download()

    val pathTo = applicationConf.getFileUrl()// "src/main/resources/"

    val downloader = new DownloadFile()

    downloader.downloadFile(downloadURL + fileToDownload,pathTo + fileToDownload)

    downloader.decompressGZ(pathTo + fileToDownload,pathTo + fileToDownload.substring(0, fileToDownload.lastIndexOf('.')))

    println("File gz cancellato: " + downloader.deleteFile(pathTo+fileToDownload))*/




    val connector = new DBConnector("src/main/resources/postgre.properties")

    val rdd = Parser.parse("src/main/resources/2018-03-01-0.json").rdd

    val finderRDD = new JSONFinderRDD()

    val maxMinRDD = new JSONMaxMinRDD()

    val result: ((Actor, String), Int) = maxMinRDD.getMaxEventPerActorFromJSON(rdd)

    println(result)

    val actorRDD = finderRDD.findActor(rdd)



    connector.saveOnDB(actorRDD.toDF(), "actor")






  }

}
