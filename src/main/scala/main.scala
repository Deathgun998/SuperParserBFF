import java.io.FileInputStream
import java.util
import java.util.Properties

import classes.{Actor, JsonRow, Payload, Repo}
import config.application.ApplicationConfig
import file.DownloadFile
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.io.FilenameUtils
import config.spark.SparkConfig
import counters.rdd.ActorCounter

object main {

  def main(args:Array[String]): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //SPARK CONGIGURATION

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    //DOWNLOAD FILE

    /*val applicationConf = new ApplicationConfig()

    val downloadURL = applicationConf.getFileUrl()

    val fileToDownload = applicationConf.getFile2Download()

    val pathTo = applicationConf.getFileUrl() "src/main/resources/"

    val downloader = new DownloadFile()

    downloader.downloadFile(downloadURL + fileToDownload,pathTo + fileToDownload)

    downloader.decompressGZ(pathTo + fileToDownload,pathTo + fileToDownload.substring(0, fileToDownload.lastIndexOf('.')))

    println("File gz cancellato: " + downloader.deleteFile(pathTo+fileToDownload))*/

    //

    val counter = new ActorCounter

    val genericRepo = new Repo(1,"repoName","repoUrl")
    val genericPayload = new Payload(1,0,0,"ref","head","before",Seq())

    val actorSeq = Seq(
      new Actor(1,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(2,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(3,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(4,"login","displayLogin","gravatarId","url","avatarUrl")
    )

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
    ))

    val result = counter.findActor(rdd)

    result.foreach(println)

  }

}
