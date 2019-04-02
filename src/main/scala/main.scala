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
import counters.rdd.JSONFinderRDD
import dao.DBConnector

object main {

  def main(args:Array[String]): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //SPARK CONGIGURATION

    System.setProperty("hadoop.home.dir", "D:/programmi/hadoop-2.6.0")

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    //DOWNLOAD FILE

    val applicationConf = new ApplicationConfig()

    val downloadURL = applicationConf.getFileUrl()

    val fileToDownload = applicationConf.getFile2Download()

    val pathTo = applicationConf.getFileUrl()// "src/main/resources/"

    val downloader = new DownloadFile()

    downloader.downloadFile(downloadURL + fileToDownload,pathTo + fileToDownload)

    downloader.decompressGZ(pathTo + fileToDownload,pathTo + fileToDownload.substring(0, fileToDownload.lastIndexOf('.')))

    println("File gz cancellato: " + downloader.deleteFile(pathTo+fileToDownload))

    val genericRepo = JSONMocks.genericRepo
    val genericPayload = JSONMocks.genericPayload

    val actorSeq = JSONMocks.actorSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt")
    ))

    val connector = new DBConnector("src/main/resources/postgre.properties")

    val finderRDD = new JSONFinderRDD()

    val actorRDD = finderRDD.findActor(rdd)

    connector.saveOnDB(actorRDD.toDF(), "actor")








  }

}
