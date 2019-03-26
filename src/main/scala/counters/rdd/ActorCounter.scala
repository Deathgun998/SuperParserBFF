package counters.rdd

import classes.{Actor, JsonRow}
import org.apache.spark.rdd.RDD

class ActorCounter {

  def findActor(jsonRawRDD: RDD[JsonRow]):RDD[Actor]={

    val res: RDD[Actor] = jsonRawRDD.groupBy(x=> x.actor).map(x=> x._1)
    res

  }

}
