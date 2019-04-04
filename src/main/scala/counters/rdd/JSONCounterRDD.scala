package counters.rdd

import java.sql.Timestamp
import java.util.Calendar

import classes.{Actor, Commit, JsonRow, Repo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class JSONCounterRDD {

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
  def countEventPerActor(rdd: RDD[JsonRow]):  RDD[(Actor, Int)] ={

    val result = rdd.map(x=>(x.actor,1)).reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def countEventPerTypeAndActor(rdd: RDD[JsonRow]) : RDD[((Actor,String),Int)]={
    val result: RDD[((Actor,String), Int)] = rdd.map(x=>((x.actor,x.eventType),1)).reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «event», divisi per «type», s«actor», «repo»;
  def countEventPerActorTypeAndRepo(rdd: RDD[JsonRow]): RDD[((Actor,String,Repo),Int)]={
    val result: RDD[((Actor,String,Repo), Int)] = rdd.map(x=>((x.actor,x.eventType,x.repo),1)).reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo» e ora;
  def countEventPerActorTypeAndRepoAndHour(rdd: RDD[(JsonRow,Int)]): RDD[((Actor,String,Repo,Int),Int)]={
    val result: RDD[((Actor,String,Repo,Int), Int)] =
      rdd
        .map(x=>((x._1.actor,x._1.eventType,x._1.repo,x._2),1))
        .reduceByKey((x, y)=> x+y )
    result
  }

  //Contare il numero di «commit»;
  def countCommit(rdd: RDD[(JsonRow)]):Long={
    val result= rdd.filter(x=> x.payload.commits != null && !x.payload.commits.isEmpty ).map(x=>x.payload.commits.size).reduce(_+_)
    result
  }

  //Contare il numero di «commit» per ogni «actor»;
  def countCommitPerActor(rdd: RDD[(JsonRow)]):RDD[(Actor,Int)]={
    val trasformed = rdd.filter(x=> x.payload.commits != null && !x.payload.commits.isEmpty ).map(x=>(x.actor,x.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }

  //Contare il numero di «commit», divisi per «type» e «actor»;
  def countCommitPerActorAndType(rdd: RDD[(JsonRow)]):RDD[((Actor,String),Int)]={
    val trasformed = rdd.filter(x=> x.payload.commits != null && !x.payload.commits.isEmpty ).map(x=>((x.actor,x.eventType),x.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }

  //Contare il numero di «commit», divisi per «type», «actor» e «event»; TODO Cazz'è?


  //Contare il numero di «commit», divisi per «type», «actor» e ora;
  def countCommitPerActorAndTypeAndHour(rdd: RDD[(JsonRow,Int)]):RDD[((Actor,String,Int),Int)]={
    val trasformed = rdd.filter(x=> x._1.payload.commits != null && !x._1.payload.commits.isEmpty ).map(x=>((x._1.actor,x._1.eventType,x._2),x._1.payload.commits.size))
    trasformed.reduceByKey(_+_)
  }

}
