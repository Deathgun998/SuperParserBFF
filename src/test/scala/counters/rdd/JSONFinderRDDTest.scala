package counters.rdd

import classes._
import com.holdenkarau.spark.testing.SharedSparkContext
import counters.JSONMethodsBasic
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.scalatest._

import scala.collection.immutable

class JSONFinderRDDTest extends FunSuite with SharedSparkContext{


  override def beforeAll(): Unit ={

    super.beforeAll()

  }

  test("findActor test") {

    val finder = new JSONFinderRDD

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload

    val actorSeq = JSONMethodsBasic.actorSeq

    val rdd = sc.parallelize(Seq(
        new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
        new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt","")
      ))

    val result = finder.findActor(rdd)

    result.foreach(println)

    assert(result.count() == actorSeq.size)

  }

  test("findAuthor test") {
    val finder = new JSONFinderRDD
    val genericRepo = JSONMethodsBasic.genericRepo
    val genericActor = JSONMethodsBasic.genericActor

    val authorSeq = JSONMethodsBasic.authorSeq

    val payloadSeq = JSONMethodsBasic.payloadSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(0),true,"createdAt",""),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(1),true,"createdAt",""),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(2),true,"createdAt",""),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(3),true,"createdAt","")
    ))

    val result = finder.findAuthor(rdd, sc)

    result.foreach(println)

    assert(result.count() == authorSeq.size)

  }

  test("findRepo test"){
    val finder = new JSONFinderRDD

    val genericActor = JSONMethodsBasic.genericActor
    val genericPayload = JSONMethodsBasic.genericPayload

    val repoSeq = JSONMethodsBasic.repoSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(1),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(3),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(4),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(4),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(2),genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",genericActor,repoSeq(5),genericPayload,true,"createdAt","")
    ))

    val result = finder.findRepo(rdd)

    result.foreach(println)

    assert(result.count() == repoSeq.size)
  }

  test("eventTypeFinder"){
    val finder = new JSONFinderRDD

    val genericActor = JSONMethodsBasic.genericActor
    val genericPayload = JSONMethodsBasic.genericPayload
    val genericRepo = JSONMethodsBasic.genericRepo

    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", eventTypeSeq(0),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),genericActor,genericRepo,genericPayload,true,"createdAt","")
    ))

    val result = finder.findEventType(rdd)

    result.foreach(println)

    assert(result.count() == eventTypeSeq.size)

  }

}
