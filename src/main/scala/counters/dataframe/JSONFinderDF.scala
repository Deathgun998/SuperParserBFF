package counters.dataframe

import classes._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Column}



class JSONFinderDF(SQLContext: SQLContext) {

  import SQLContext.implicits._
  import org.apache.spark.sql.functions._

  def findColumnDF(column: Column, df: DataFrame): DataFrame ={
    df.select(column).distinct()
  }

  //Trovare i singoli «actor»
  def findActor(jsonRow: DataFrame):DataFrame={

    findColumnDF($"actor",jsonRow)

  }

  //Trovare i singoli «author»
  def findAuthor(jsonRow: DataFrame, sc: SparkContext): DataFrame ={

    jsonRow.select(explode(col("payload.commits.author")).as("author")).distinct()

  }

  //Trovare i singoli «repo»
  def findRepo(jsonRow: DataFrame):DataFrame={
    findColumnDF($"repo",jsonRow)
  }

  //Trovare i vari tipi di evento «type»
  def findEventType(jsonRow: DataFrame): DataFrame ={
    findColumnDF($"eventType",jsonRow)
  }

}
