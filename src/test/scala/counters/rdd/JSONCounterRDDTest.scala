package counters.rdd

import classes.{Actor, JsonRow, Repo}
import com.holdenkarau.spark.testing.SharedSparkContext
import counters.JSONMethodsBasic
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class JSONCounterRDDTest extends FunSuite with SharedSparkContext {

  test("countEventPerActor test") {

    val counter = new JSONCounterRDD

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
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload,true,"createdAt","")
    ))

    val result: RDD[(Actor, Int)] = counter.countEventPerActor(rdd)

    result.map(x=>(x._1.id,x._2)).foreach(println)

    assert(result.collect().forall(x=>
      (x._1.id == 0 && x._2 == 7) || // push
        (x._1.id == 1 && x._2 == 10) || // fetch
        (x._1.id == 2 && x._2 == 6) || // pull
        (x._1.id == 3 && x._2 == 4) // update
    ))



  }

  test("countEventPerTypeAndActor test") {

    val counter = new JSONCounterRDD

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload

    val actorSeq = JSONMethodsBasic.actorSeq

    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo,genericPayload,true,"createdAt","")
    ))

    val result: RDD[((Actor, String), Int)] = counter.countEventPerTypeAndActor(rdd)

    result.map(x=>((x._1._1.id,x._1._2),x._2)).foreach(println)

    assert(result.filter(x=> x._1._1.id == 0).collect().forall(x=>
        ((x._1._2.equals(eventTypeSeq(0))) && x._2 == 2) || // push
        ((x._1._2.equals(eventTypeSeq(1))) && x._2 == 3) || // fetch
        ((x._1._2.equals(eventTypeSeq(2))) && x._2 == 1) || // pull
        ((x._1._2.equals(eventTypeSeq(3))) && x._2 == 1) // update
    ))



  }

  test("countEventPerActorTypeAndRepo test") {

    val counter = new JSONCounterRDD

    val genericRepo1 = JSONMethodsBasic.genericRepo
    val genericRepo2 = JSONMethodsBasic.genericRepo.copy(id=5)
    val genericPayload = JSONMethodsBasic.genericPayload

    val actorSeq = JSONMethodsBasic.actorSeq

    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(3),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo2,genericPayload,true,"createdAt","")
    ))

    val result: RDD[((Actor, String, Repo), Int)] = counter.countEventPerActorTypeAndRepo(rdd)

    result.map(x=>((x._1._1.id,x._1._2,x._1._3.id),x._2)).foreach(println)

    assert(result.filter(x=> x._1._1.id == 0).collect().forall(x=>
      ((x._1._2.equals(eventTypeSeq(0))) && x._1._3.id == genericRepo1.id && x._2 == 1) || // push
        ((x._1._2.equals(eventTypeSeq(0))) && x._1._3.id == genericRepo2.id && x._2 == 1) || // push
        ((x._1._2.equals(eventTypeSeq(1))) && x._1._3.id == genericRepo1.id && x._2 == 1) || // fetch
        ((x._1._2.equals(eventTypeSeq(1))) && x._1._3.id == genericRepo2.id && x._2 == 2) || // fetch
        ((x._1._2.equals(eventTypeSeq(2))) && x._2 == 1) || // pull
        ((x._1._2.equals(eventTypeSeq(3))) && x._2 == 1) // update
    ))



  }

  test("countEventPerActorTypeAndRepoAndHour test") {

    val counter = new JSONCounterRDD

    val genericRepo1 = JSONMethodsBasic.genericRepo
    val genericRepo2 = JSONMethodsBasic.genericRepo.copy(id=5)
    val genericPayload = JSONMethodsBasic.genericPayload

    val actorSeq = JSONMethodsBasic.actorSeq

    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd: RDD[(JsonRow, Int)] = sc.parallelize(Seq(
      (new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),2),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),2),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),2),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),2),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(2),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(2),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(2),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(3),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(3),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(3),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(3),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(0),actorSeq(3),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(2),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo1,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),1),
      (new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo2,genericPayload,true,"createdAt",""),1)
    ))

    val result: RDD[((Actor, String, Repo,Int), Int)] = counter.countEventPerActorTypeAndRepoAndHour(rdd)

    result.map(x=>((x._1._1.id,x._1._2,x._1._3.id,x._1._4),x._2)).foreach(println)

    assert(result.filter(x=> x._1._1.id == 0).collect().forall(x=>
      ((x._1._2.equals(eventTypeSeq(0))) && x._1._3.id == genericRepo1.id && x._2 == 1) || // push
        ((x._1._2.equals(eventTypeSeq(0))) && x._1._3.id == genericRepo2.id && x._2 == 1 && x._1._4 == 1) || // push
        ((x._1._2.equals(eventTypeSeq(0))) && x._1._3.id == genericRepo2.id && x._2 == 1 && x._1._4 == 2) || // push
        ((x._1._2.equals(eventTypeSeq(1))) && x._1._3.id == genericRepo1.id && x._2 == 1) || // fetch
        ((x._1._2.equals(eventTypeSeq(1))) && x._1._3.id == genericRepo2.id && x._2 == 2) || // fetch
        ((x._1._2.equals(eventTypeSeq(2))) && x._2 == 1) || // pull
        ((x._1._2.equals(eventTypeSeq(3))) && x._2 == 1) // update
    ))



  }

  test("countCommit test") {

    val counter = new JSONCounterRDD

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload
    val actorSeq = JSONMethodsBasic.actorSeq
    val commitsSeq = JSONMethodsBasic.commitSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(0),commitsSeq(1))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(2),commitsSeq(3))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(4),commitsSeq(5))),true,"createdAt","")
    ))

    val result = counter.countCommit(rdd)

    println("commits count: " + result)

    val usedCommits = 6

    assert(result == usedCommits)
  }

  test("countCommitPerActor test") {

    val counter = new JSONCounterRDD

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload
    val actorSeq = JSONMethodsBasic.actorSeq
    val commitsSeq = JSONMethodsBasic.commitSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(0),commitsSeq(1))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(2),commitsSeq(3))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(4),commitsSeq(5))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(6))),true,"createdAt",""),
      new JsonRow("1", "1",actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(7),commitsSeq(8),commitsSeq(9))),true,"createdAt","")
    ))

    val result: RDD[(Actor, Int)] = counter.countCommitPerActor(rdd)

    result.foreach(println)

    result.collect().forall(x=>
      (x._1.id == actorSeq(0).id && x._2 == 6)
        || (x._1.id == actorSeq(0).id && x._2 == 4)
    )
  }

  test("countCommitPerActorAndType test"){
    val counter = new JSONCounterRDD

    val genericRepo = JSONMethodsBasic.genericRepo
    val genericPayload = JSONMethodsBasic.genericPayload
    val actorSeq = JSONMethodsBasic.actorSeq
    val commitsSeq = JSONMethodsBasic.commitSeq
    val eventTypeSeq = JSONMethodsBasic.eventTypeSeq

    val rdd = sc.parallelize(Seq(
      new JsonRow("1", eventTypeSeq(0),actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(0),commitsSeq(1))),true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(0),actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(2),commitsSeq(3))),true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(1),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(4),commitsSeq(5))),true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(1),actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(6))),true,"createdAt",""),
      new JsonRow("1", eventTypeSeq(2),actorSeq(0),genericRepo,genericPayload.copy(commits = Seq(commitsSeq(7),commitsSeq(8),commitsSeq(9))),true,"createdAt","")
    ))

    val result: RDD[((Actor, String), Int)] = counter.countCommitPerActorAndType(rdd)

    result.foreach(println)

    result.collect().forall(x=>
      (x._1._1.id == actorSeq(0).id && x._1._2.equals(eventTypeSeq(0))&& x._2 == 2)
        || (x._1._1.id == actorSeq(0).id && x._1._2.equals(eventTypeSeq(1))&& x._2 == 1)
        || (x._1._1.id == actorSeq(0).id && x._1._2.equals(eventTypeSeq(2))&& x._2 == 3)
        || (x._1._1.id == actorSeq(1).id && x._1._2.equals(eventTypeSeq(0))&& x._2 == 2)
        || (x._1._1.id == actorSeq(1).id && x._1._2.equals(eventTypeSeq(1))&& x._2 == 2)
    )

  }

}
