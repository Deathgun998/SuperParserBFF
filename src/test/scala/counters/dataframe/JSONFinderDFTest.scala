package counters.dataframe

import classes.JsonRow
import com.holdenkarau.spark.testing.SharedSparkContext
import counters.JSONMethodsBasic
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class JSONFinderDFTest extends FunSuite with SharedSparkContext {

  override def beforeAll(): Unit ={
    super.beforeAll()
  }

  test("findActor test") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val counter = new JSONFinderDF(sqlContext)

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload

    val actorSeq = JSONMethodsBasic.actorSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt")
    ))

    val result = counter.findActor(rdd.toDF())

    result.show()

    assert(result.count() == actorSeq.size)

  }

  test("findAuthor test") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val counter = new JSONFinderDF(sqlContext)


    val genericRepo = JSONMethodsBasic.genericRepo
    val genericActor = JSONMethodsBasic.genericActor

    val authorSeq = JSONMethodsBasic.authorSeq

    val payloadSeq = JSONMethodsBasic.payloadSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(0),true,"createdAt"),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(1),true,"createdAt"),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(2),true,"createdAt"),
      new JsonRow("1", "1",genericActor,genericRepo,payloadSeq(3),true,"createdAt")
    ))

    val result = counter.findAuthor(rdd.toDF(), sc)

    result.show()

    assert(result.count() == authorSeq.size)

  }

  test("findRepo test"){

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val counter = new JSONFinderDF(sqlContext)

    val genericActor = JSONMethodsBasic.genericActor
    val genericPayload = JSONMethodsBasic.genericPayload

    val repoSeq = JSONMethodsBasic.repoSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(1),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(3),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(4),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(0),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(4),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(2),genericPayload,true,"createdAt"),
      new JsonRow("1", "1",genericActor,repoSeq(5),genericPayload,true,"createdAt")
    ))

    val result = counter.findRepo(rdd.toDF())

    result.show()

    assert(result.count() == repoSeq.size)
  }

  test("eventTypeFinder"){

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val counter = new JSONFinderDF(sqlContext)

    val genericActor = JSONMethodsBasic.genericActor
    val genericPayload = JSONMethodsBasic.genericPayload
    val genericRepo = JSONMethodsBasic.genericRepo

    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", eventTypeSeq(0),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(2),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(3),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(2),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(1),genericActor,genericRepo,genericPayload,true,"createdAt"),
      new JsonRow("1", eventTypeSeq(0),genericActor,genericRepo,genericPayload,true,"createdAt")
    ))

    val result = counter.findEventType(rdd.toDF)

    result.show()

    assert(result.count() == eventTypeSeq.size)

  }
}
