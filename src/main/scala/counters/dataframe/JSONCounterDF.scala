package counters.dataframe

import classes.{Actor, JsonRow, Repo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.ClassTag

class JSONCounterDF (SQLContext: SQLContext) {

  import SQLContext.implicits._
  import org.apache.spark.sql.functions._

  def countActor(actors: DataFrame): Long ={
    actors.count()
  }

  def countRepo(repos: DataFrame): Long={
    repos.count()
  }

  //Contare il numero di «event» per ogni «actor»;
  def countEventPerActor(df: DataFrame):  DataFrame ={

    df.groupBy(col("actor")).count()

  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def countEventPerTypeAndActor(df: DataFrame) : DataFrame={

    df.groupBy(col("actor"),col("eventType")).count()

  }

  //Contare il numero di «event», divisi per «type», s«actor», «repo»;
  def countEventPerActorTypeAndRepo(df: DataFrame) : DataFrame={
    df.groupBy(col("actor"),col("eventType"),col("repo")).count()
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo» e ora;
  def countEventPerActorTypeAndRepoAndHour(df: DataFrame) : DataFrame={
    df.groupBy(col("actor"),col("eventType"),col("repo"),col("hour")).count()
  }

  //Contare il numero di «commit»;
  def countCommit(df: DataFrame) : Long={
    df.select(explode(col("payload.commits")).as("commit")).count()
  }

  //Contare il numero di «commit» per ogni «actor»;
  def countCommitPerActor(df: DataFrame):DataFrame={

    df.select(col("actor"),explode(col("payload.commits")).as("commit")).groupBy(col("actor")).count()
  }

  //Contare il numero di «commit», divisi per «type» e «actor»;
  def countCommitPerActorAndType(df: DataFrame):DataFrame={
    df
      .select(col("actor"),col("eventType"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("actor"),col("eventType"))
      .count()
  }

  //Contare il numero di «commit», divisi per «type», «actor» e «event»; TODO Cazz'è?


  //Contare il numero di «commit», divisi per «type», «actor» e ora;
  def countCommitPerActorAndTypeAndHour(df: DataFrame):DataFrame={
    df
      .select(col("actor"),col("eventType"),col("hour"),explode(col("payload.commits")).as("commit"))
      .groupBy(col("actor"),col("eventType"),col("hour"))
      .count()


  }
}
