import java.awt.Event

import classes.JsonRow
import config.spark.SparkConfig
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Parser {

  def parse(outputFile: String): Dataset[JsonRow] ={

    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    implicit val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .config(conf)
      .getOrCreate()

    val eventEncoder = Encoders.product[JsonRow]

    val input: DataFrame = spark.read.option("inferSchema", true)
      .json(outputFile).withColumnRenamed("public", "isPublic").withColumnRenamed("type", "eventType")

    val eventDS: Dataset[JsonRow] = input.as[JsonRow](eventEncoder)

    eventDS
  }
}
