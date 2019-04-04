package counters.rdd

import java.sql.Timestamp
import java.util.Calendar

import classes._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class JSONFinderRDD {

  //Trovare i singoli «actor»
  def findActor(jsonRow: RDD[JsonRow]):RDD[Actor]={

    val res: RDD[Actor] = jsonRow.groupBy(x=> x.actor).map(x=> x._1)
    res

  }

  //Trovare i singoli «author»
  def findAuthor(jsonRow: RDD[JsonRow], sc: SparkContext): RDD[Author] ={

    val commits: RDD[Commit] = jsonRow.filter(x=> x.payload !=null && x.payload.commits != null)
      .map(x=> x.payload.commits)
      .flatMap(x=> x)

    val authors = commits.map(x=>x.author).distinct()
    authors
  }

  //Trovare i singoli «repo»
  def findRepo(jsonRow: RDD[JsonRow]):RDD[Repo]={
    val res: RDD[Repo] = jsonRow.groupBy(x=> x.repo).map(x=> x._1)
    res
  }

  //Trovare i vari tipi di evento «type»
  def findEventType(jsonRow: RDD[JsonRow]):RDD[String] ={
    val res = jsonRow.groupBy(x=> x.eventType).map(x=>x._1)
    res
  }


}
