package counters.rdd

import classes.{Actor, JsonRow, Repo}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class JSONMaxMinRDD {

  def counterRDD = new JSONCounterRDD()

  private def getMax[V: ClassTag](rdd: RDD[((V),Int)]): ((V),Int) ={
    rdd.reduce((x, y)=> if(x._2>=y._2)x else y)
  }

  private def getMin[V: ClassTag](rdd: RDD[((V),Int)]): ((V),Int) ={
    rdd.reduce((x, y)=> if(x._2<=y._2)x else y)
  }

  //Trovare il massimo/minimo numero di «event» per «actor»;
  //da countEventPerTypeAndActor
  def getMaxEventPerActor(rdd:RDD[((Actor, String), Int)] ): ((Actor, String), Int) ={
    val max: ((Actor, String), Int) = getMax(rdd)

    max
  }
  //da countEventPerTypeAndActor
  def getMinEventPerActor(rdd: RDD[((Actor, String), Int)] ): ((Actor, String), Int) ={
    val min: ((Actor, String), Int) = getMin(rdd)

    min
  }


  //Trovare il massimo/minimo numero di «event» per «repo»;
  def getMaxEventPerRepo(rdd:RDD[JsonRow]): ((Repo, String), Int) ={
    val reposCount: RDD[((Repo, String), Int)] = rdd
      .map(x=>((x.repo,x.eventType),1))
      .reduceByKey((x,y)=>x+y)
    val max = getMax(reposCount)
    max
  }
  def getMinEventPerRepo(rdd:RDD[JsonRow]): ((Repo, String), Int) ={
    val reposCount: RDD[((Repo, String), Int)] = rdd.map(x=>((x.repo,x.eventType),1)).reduceByKey((x, y)=>x+y)
    val min = getMin(reposCount)
    min
  }

  //TODO da testare
  //Trovare il massimo/minimo numero di «event» per ora ora per «actor»;
  def getMaxEventPerHourAndActor(rdd: RDD[(JsonRow,Int)]):RDD[(((Actor,String),Int),Int)]={
    val eventPerHourAndActor = rdd
      .map(x=>(((x._1.actor,x._1.eventType),x._2),1))
      .reduceByKey((x,y)=>x+y)
    val max: RDD[(((Actor,String),Int),Int)] = eventPerHourAndActor
      .map(x=>((x._1._1._1,x._1._2),(x._1._1._2,x._2)))
      .reduceByKey((x,y)=>
        if (x._2>=y._2) x
        else y).map(x=>(((x._1._1,x._2._1),x._1._2),x._2._2))
    max
  }
  def getMinEventPerHourAndActor(rdd: RDD[(JsonRow,Int)]):RDD[(((Actor,String),Int),Int)]={
    val eventPerHourAndActor = rdd
      .map(x=>(((x._1.actor,x._1.eventType),x._2),1))
      .reduceByKey((x,y)=>x+y)
    val min: RDD[(((Actor,String),Int),Int)] = eventPerHourAndActor
      .map(x=>((x._1._1._1,x._1._2),(x._1._1._2,x._2)))
      .reduceByKey((x,y)=>
        if (x._2<=y._2)
          x
        else
          y)
      .map(x=>(((x._1._1,x._2._1),x._1._2),x._2._2))
    min
  }

  //Trovare il massimo/minimo numero di «commit» per ora per «repo»;
  def getMaxCommittPerHourAndRepo(rdd: RDD[(JsonRow,Int)]): RDD[((Repo, Int), Int)] ={
    val trasformed = rdd.map(x=> ((x._1.repo,x._2),x._1.payload.commits.size))
    val max = trasformed.reduceByKey((x,y)=> if (x >=y) x else y)
    max
  }
  def getMinCommittPerHourAndRepo(rdd: RDD[(JsonRow,Int)]): RDD[((Repo, Int), Int)] = {
    val trasformed = rdd.map(x=> ((x._1.repo,x._2),x._1.payload.commits.size))
    val min = trasformed.reduceByKey((x, y)=> if (x <=y) x else y)
    min
  }

  //Trovare il massimo/minimo numero di «commit» per ora per «repo» e «actor»;
  def getMaxCommitPerHourAndRepoAndActor(rdd: RDD[(JsonRow,Int)]): RDD[((Repo, Actor, Int), Int)] ={
    val trasformed = rdd.map(x=> ((x._1.repo,x._1.actor,x._2),x._1.payload.commits.size))
    val max = trasformed.reduceByKey((x, y)=> if (x >=y) x else y)
    max
  }
  def getMinCommitPerHourAndRepoAndActor(rdd: RDD[(JsonRow,Int)]): RDD[((Repo, Actor, Int), Int)] ={
    val trasformed = rdd.map(x=> ((x._1.repo,x._1.actor,x._2),x._1.payload.commits.size))
    val min = trasformed.reduceByKey((x, y)=> if (x <=y) x else y)
    min
  }
}
