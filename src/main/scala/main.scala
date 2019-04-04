import java.io.FileInputStream
import java.sql.Connection
import java.util
import java.util.Properties

import classes.Actor
import config.application.ApplicationConfig
import file.DownloadFile
import config.spark.SparkConfig
import counters.rdd.{JSONCounterRDD, JSONFinderRDD, JSONMaxMinRDD}
import dao.{CountDL, DBConnector, MaxMinDL}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object main {

  def main(args:Array[String]): Unit ={


    //SPARK CONGIGURATION

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    implicit val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //DOWNLOAD FILE

    val applicationConf = new ApplicationConfig("src/main/resources/application.properties")

    val downloadURL = applicationConf.getFileUrl()

    val fileToDownload = applicationConf.getFile2Download()

    val pathTo = applicationConf.getFileDestination()// "src/main/resources/"

    val destination = pathTo + fileToDownload

    val downloader = new DownloadFile()

    downloader.downloadFile(downloadURL + fileToDownload,destination)

    downloader.decompressGZ(pathTo + fileToDownload,pathTo + fileToDownload.substring(0, fileToDownload.lastIndexOf('.')))

    val connector = new DBConnector("src/main/resources/postgre.properties")

    val rdd = Parser.parse(destination).rdd

    val rddWithHour = rdd.map(x=> (x,0))

    //controllers di count e maxmin

    val finderRDD = new JSONFinderRDD()

    val counter = new JSONCounterRDD

    val maxMinRDD = new JSONMaxMinRDD()



    //metodi di find

    val actors: RDD[Actor] = finderRDD.findActor(rdd)

    val authors = finderRDD.findAuthor(rdd, spark.sparkContext)

    val eventTypes = finderRDD.findEventType(rdd)

    val repos = finderRDD.findRepo(rdd)

    connector.saveOnDB(actors.toDF(), "actor")
    connector.saveOnDB(authors.toDF(), "author")
    connector.saveOnDB(eventTypes.toDF(), "eventType")
    connector.saveOnDB(repos.toDF(), "repo")


    //metodi di count


    val reposCount = counter.countRepo(repos)
    val reposCountDL = new CountDL("reposCount","",reposCount)

    val commitCount = counter.countCommit(rdd)
    val commitCountDL = new CountDL("commitCount","",commitCount)

    val countCommitPerActor = counter.countCommitPerActor(rdd)
    val countCommitPerActorDL = countCommitPerActor.map(x=> new CountDL("countCommitPerActorDL","actor: " + x._1.id, x._2))

    val countCommitPerActorAndType = counter.countCommitPerActorAndType(rdd)
    val countCommitPerActorAndTypeDL = countCommitPerActorAndType
      .map(x=> new CountDL("countCommitPerActorAndType","actor: " + x._1._1.id + " eventType: " + x._1._2, x._2))

    val countEventPerActor: RDD[(Actor, Int)] = counter.countEventPerActor(rdd)
    val countEventPerActorDL = countEventPerActor.map(x=> new CountDL("countEventPerActor","actor: " + x._1,x._2))

    val countEventPerTypeAndActor = counter.countEventPerTypeAndActor(rdd)
    val countEventPerTypeAndActorDL = countEventPerTypeAndActor
      .map(x=> new CountDL("countEventPerTypeAndActor","actor: " + x._1._1.id + " eventType: " + x._1._2,x._2))

    val countEventPerActorTypeAndRepo = counter.countEventPerActorTypeAndRepo(rdd)
    val countEventPerActorTypeAndRepoDL = countEventPerActorTypeAndRepo
      .map(x=> new CountDL("countEventPerActorTypeAndRepo","actor: " + x._1._1.id + " eventType: " + x._1._2 + " repo: " + x._1._3.id ,x._2))


    //metodi max min
    val maxEventPerActor = maxMinRDD.getMaxEventPerActor(countEventPerTypeAndActor)
    val maxEventPerActorDL = new MaxMinDL(
      "maxEventPerActor",
      ("actor: " + maxEventPerActor._1._1.id + " type: " + maxEventPerActor._1._2),
      maxEventPerActor._2,
      "MAX")
    val minEventPerActor = maxMinRDD.getMinEventPerActor(countEventPerTypeAndActor)
    val minEventPerActorDL = new MaxMinDL(
      "minEventPerActor",
      ("actor: " + maxEventPerActor._1._1.id + " type: " + maxEventPerActor._1._2),
      maxEventPerActor._2,
      "MIN")
    val maxEventPerHourAndActor = maxMinRDD.getMaxEventPerHourAndActor(rddWithHour)
      .map(x=>new MaxMinDL(
        "maxEventPerHourAndActor",
        ("actor: " + x._1._1._1.id + " type: " + x._1._1._2 + " hour:" +x._1._2),
        x._2,
        "MAX"))
    val minEventPerHourAndActor = maxMinRDD.getMinEventPerHourAndActor(rddWithHour)
      .map(x=>new MaxMinDL("minEventPerHourAndActor",
        ("actor: " + x._1._1._1.id + " type: " + x._1._1._2 + " hour:" +x._1._2),
        x._2,
        "MIN"))
    val maxCommittPerHourAndRepo = maxMinRDD.getMaxCommittPerHourAndRepo(rddWithHour)
    val minCommittPerHourAndRepo = maxMinRDD.getMinCommittPerHourAndRepo(rddWithHour)
    val maxCommitPerHourAndRepoAndActor = maxMinRDD.getMaxCommitPerHourAndRepoAndActor(rddWithHour)
    val minCommitPerHourAndRepoAndActor = maxMinRDD.getMinCommitPerHourAndRepoAndActor(rddWithHour)




/*

    val df = rdd.toDF()
    val dfWithHour = df.withColumn("hour", lit(0))

    //controllers di count e maxmin

    val finderDF = new JSONFinderDF(spark.sqlContext)

    val counterDF = new JSONCounterDF(spark.sqlContext)

    val maxMinDF = new JSONMaxMinDF(spark.sqlContext)



    //metodi di find


    val actorsDF = finderDF.findActor(df)


    val authorsDF = finderDF.findAuthor(df,spark.sparkContext)


    val eventTypesDF = finderDF.findEventType(df)


    val reposDF = finderDF.findRepo(df)

    //metodi di count

    val reposCountDF = counterDF.countRepo(repos.toDF())

    val commitCountDF = counterDF.countCommit(rdd.toDF())

    val countCommitPerActorDF = counterDF.countCommitPerActor(df)

    val countCommitPerActorAndTypeDF = counterDF.countCommitPerActorAndType(df)

    val countEventPerPerActorDF = counterDF.countEventPerActor(df)

    val countEventPerTypeAndActorDF = counterDF.countEventPerTypeAndActor(df)

    val countEventPerActorTypeAndRepoDF = counterDF.countEventPerActorTypeAndRepo(df)


    //metodi max min


    val maxEventPerActorDF = maxMinDF.getMaxEventPerActor(countEventPerTypeAndActorDF)
    val minEventPerActorDF = maxMinDF.getMinEventPerActor(countEventPerTypeAndActorDF)
    val maxEventPerHourAndActorDF = maxMinDF.getMaxEventPerHourAndActor(dfWithHour)
    val minEventPerHourAndActorDF = maxMinDF.getMinEventPerHourAndActor(dfWithHour)
    val maxCommittPerHourAndRepoDF = maxMinDF.getMaxCommittPerHourAndRepo(dfWithHour)
    val minCommittPerHourAndRepoDF = maxMinDF.getMinCommittPerHourAndRepo(dfWithHour)
    val maxCommitPerHourAndRepoAndActorDF = maxMinDF.getMaxCommitPerHourAndRepoAndActor(dfWithHour)
    val minCommitPerHourAndRepoAndActorDF = maxMinDF.getMinCommitPerHourAndRepoAndActor(dfWithHour)
*/
  }

}
