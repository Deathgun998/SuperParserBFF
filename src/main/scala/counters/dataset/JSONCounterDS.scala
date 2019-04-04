package counters.dataset

import classes.{Actor, JsonRow, Repo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

class JSONCounterDS(sparkSession: SparkSession) {

  import sparkSession.implicits._

  private def rddCount[V: ClassTag ](rdd: RDD[V]): Long={
    rdd.count()
  }

  def countActor(rdd: RDD[Actor]): Long ={
    rddCount(rdd)
  }

  def countRepo(rdd: RDD[Repo]): Long={
    rddCount(rdd)
  }

  //Contare il numero di «event» per ogni «actor»;
  def countEventPerActor(ds: Dataset[JsonRow]):  Dataset[(Actor, Int)] ={

    val result: Dataset[(Actor, Int)] = ds
      .map(x=>(x.actor,1))
      .groupByKey(_._1)
      .reduceGroups((x, y)=>(x._1,x._2+y._2))
      .map(_._2)
    result
  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def countEventPerTypeAndActor(ds: Dataset[JsonRow]) : Dataset[((Actor,String),Int)]={

    val result= ds
      .map(x=>((x.actor,x.eventType),1))
      .groupByKey(_._1)
      .reduceGroups((x,y) =>(x._1,x._2+y._2))
      .map(_._2)

    result
  }
/*
  //Contare il numero di «event», divisi per «type», s«actor», «repo»;
  def countEventPerActorTypeAndRepo(ds: Dataset[JsonRow]): Dataset[((Actor,String,Repo),Int)]={
    val result: RDD[((Actor,String,Repo), Int)] = ds.map(x=>((x.actor,x.eventType,x.repo),1)).reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo» e ora;
  def countEventPerActorTypeAndRepoAndHour(ds: Dataset[(JsonRow,Int)]): Dataset[((Actor,String,Repo,Int),Int)]={
    val result: RDD[((Actor,String,Repo,Int), Int)] =
      ds
        .map(x=>((x._1.actor,x._1.eventType,x._1.repo,x._2),1))
        .reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «commit»;
  def countCommit(ds: Dataset[(JsonRow)], sc: SparkContext):Long={
    val result= ds.map(x=>x.payload.commits.size).reduce(_+_)
    result
  }

  //Contare il numero di «commit» per ogni «actor»;
  def countCommitPerActor(ds: Dataset[(JsonRow)], sc: SparkContext):RDD[(Actor,Int)]={
    val trasformed = ds.map(x=>(x.actor,x.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }

  //Contare il numero di «commit», divisi per «type» e «actor»;
  def countCommitPerActorAndType(ds: Dataset[(JsonRow)], sc: SparkContext):Dataset[((Actor,String),Int)]={
    val trasformed = ds.map(x=>((x.actor,x.eventType),x.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }

  //Contare il numero di «commit», divisi per «type», «actor» e «event»; TODO Cazz'è?


  //Contare il numero di «commit», divisi per «type», «actor» e ora;
  def countCommitPerActorAndTypeAndHour(ds: Dataset[(JsonRow,Int)], sc: SparkContext):Dataset[((Actor,String,Int),Int)]={
    val trasformed = ds.map(x=>((x._1.actor,x._1.eventType,x._2),x._1.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }
*/
}
