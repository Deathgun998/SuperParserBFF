package counters.rdd

import classes.{Actor, JsonRow, Payload, Repo}
import config.spark.SparkConfig
import org.apache.spark.SparkContext
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.scalatest.junit.AssertionsForJUnit

//@RunWith(classOf[JUnit4])
class ActorCountTest extends AssertionsForJUnit {

  def init: SparkContext = {
    val sparkPropFile = "src/main/resources/sparkConfig.properties"

    val sparkConfigurator = new SparkConfig()

    val conf = sparkConfigurator.loadSparkProps(sparkPropFile)

    val sc = new SparkContext(conf)

    sc
  }

  @Test
  def ActorCountTest: Unit ={

    val sc = init

    val counter = new ActorCounter

    val genericRepo = new Repo(1,"repoName","repoUrl")
    val genericPayload = new Payload(1,0,0,"ref","head","before",Seq())

    val actorSeq = Seq(
      new Actor(1,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(2,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(3,"login","displayLogin","gravatarId","url","avatarUrl"),
      new Actor(4,"login","displayLogin","gravatarId","url","avatarUrl")
    )

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
    ))

    val result = counter.findActor(rdd)

    result.foreach(println)

    assert(result.count() == actorSeq.size)
  }

}
