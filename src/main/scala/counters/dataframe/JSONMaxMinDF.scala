package counters.dataframe

import classes.{Actor, JsonRow, Repo}
import counters.rdd.JSONCounterRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Column}

import scala.reflect.ClassTag

class JSONMaxMinDF(sqlContext: SQLContext) {

  def counterRDD = new JSONCounterDF(sqlContext)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  //Trovare il massimo/minimo numero di «event» per «actor»;
  def getMaxEventPerActorFromJSON(df: DataFrame): DataFrame ={

    //actor,type,count
    val trasformed: DataFrame = counterRDD.countEventPerTypeAndActor(df)
    val max = trasformed.groupBy(col("actor")).max("count")

    max
  }
  def getMaxEventPerActor(df: DataFrame): DataFrame ={
    val max = df.groupBy(col("actor")).max("count")

    max
  }
  def getMinEventPerActorFromJSON(df: DataFrame): DataFrame ={
    val trasformed: DataFrame = counterRDD.countEventPerTypeAndActor(df)
    val min =trasformed.groupBy(col("actor")).min("count")

    min
  }
  def getMinEventPerActor(df: DataFrame): DataFrame ={
    val min = df.groupBy(col("actor")).min("count")

    min
  }


  //Trovare il massimo/minimo numero di «event» per «repo»;
  def getMaxEventPerRepo(df: DataFrame): DataFrame={
    val trasformed: DataFrame = df.groupBy("repo","eventType").count()
    val max = trasformed.groupBy(col("repo"),col("eventType")).max("count")

    max
  }
  def getMinEventPerRepo(df: DataFrame): DataFrame={
    val trasformed: DataFrame = df.groupBy("repo","eventType").count()
    val max = trasformed.groupBy(col("repo"),col("eventType")).min("count")

    max
  }

  //TODO da testare
  //Trovare il massimo/minimo numero di «event» per ora ora per «actor»;
  def getMaxEventPerHourAndActor(df: DataFrame): DataFrame={
    df.groupBy("actor","hour").count().groupBy("actor","hour").max("count")
  }
  def getMinEventPerHourAndActor(df: DataFrame): DataFrame={
    df.groupBy("actor","hour").count().groupBy("actor","hour").min("count")
  }

  //Trovare il massimo/minimo numero di «commit» per ora per «repo»;
  def getMaxCommittPerHourAndRepo(df: DataFrame): DataFrame ={
    val result = df
      .select(col("repo"),col("hour"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("repo"),col("hour")).count()
      .groupBy(col("repo"),col("hour")).max("count")

    result
  }
  def getMinCommittPerHourAndRepo(df: DataFrame): DataFrame = {
    val result = df
      .select(col("repo"),col("hour"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("repo"),col("hour")).count()
      .groupBy(col("repo"),col("hour")).min("count")

    result
  }

  //Trovare il massimo/minimo numero di «commit» per ora per «repo» e «actor»;
  def getMaxCommitPerHourAndRepoAndActor(df: DataFrame): DataFrame ={
    val result = df
      .select(col("repo"),col("actor"),col("hour"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("repo"),col("actor"),col("hour")).count()
      .groupBy(col("repo"),col("actor"),col("hour")).max("count")

    result
  }
  def getMinCommitPerHourAndRepoAndActor(df: DataFrame): DataFrame ={
    val result = df
      .select(col("repo"),col("actor"),col("hour"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("repo"),col("actor"),col("hour")).count()
      .groupBy(col("repo"),col("actor"),col("hour")).min("count")

    result
  }

}
