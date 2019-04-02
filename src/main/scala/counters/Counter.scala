package counters

import classes.{Actor, JsonRow}
import org.apache.spark.rdd.RDD

trait Counter {

  type T

  def findActor(jsonRow: RDD[JsonRow]): RDD[Actor]





}
