package counters

import classes._

object JSONMethodsBasic {

  def genericRepo = new Repo(1,"repoName","repoUrl")
  def genericActor = new Actor(1,"login","displayLogin","gravatarId","url","avatarUrl")
  def genericPayload = new Payload(1,0,0,"ref","head","before",Seq())
  def actorSeq = Seq(
    new Actor(0,"login","displayLogin","gravatarId","url","avatarUrl"),
    new Actor(1,"login","displayLogin","gravatarId","url","avatarUrl"),
    new Actor(2,"login","displayLogin","gravatarId","url","avatarUrl"),
    new Actor(3,"login","displayLogin","gravatarId","url","avatarUrl")
  )

  def authorSeq = Seq(
    new Author("name1","email1"),
    new Author("name2","email2"),
    new Author("name3","email3"),
    new Author("name4","email4"),
    new Author("name5","email5"),
    new Author("name6","email6"))

  def repoSeq = Seq(
    new Repo(0, "name1", "url1"),
    new Repo(1, "name1", "url1"),
    new Repo(2, "name1", "url1"),
    new Repo(3, "name1", "url1"),
    new Repo(4, "name1", "url1"),
    new Repo(5, "name1", "url1")
  )

  def eventTypeSeq = Seq("push","fetch","pull","update")

  def payloadSeq = Seq(
    new Payload(1,0,0,"ref","head","before",Seq(
      new Commit("sha", authorSeq(0), "message1", true, "url"),
      new Commit("sha", authorSeq(1), "message2", true, "url"),
      new Commit("sha", authorSeq(1), "message3", true, "url")
    )),
    new Payload(2,0,0,"ref","head","before",Seq(
      new Commit("sha", authorSeq(2), "message4", true, "url"),
      new Commit("sha", authorSeq(1), "message5", true, "url"),
      new Commit("sha", authorSeq(4), "message6", true, "url")
    )),
    new Payload(3,0,0,"ref","head","before",Seq(
      new Commit("sha", authorSeq(0), "message7", true, "url"),
      new Commit("sha", authorSeq(5), "message8", true, "url"),
      new Commit("sha", authorSeq(3), "message9", true, "url")
    )),
    new Payload(4,0,0,"ref","head","before",Seq(
      new Commit("sha", authorSeq(1), "message10", true, "url"),
      new Commit("sha", authorSeq(2), "message11", true, "url"),
      new Commit("sha", authorSeq(4), "message12", true, "url")
    ))
  )

  def commitSeq= Seq(
    new Commit("sha", authorSeq(0),"message0",true,"url"),
    new Commit("sha", authorSeq(0),"message1",true,"url"),
    new Commit("sha", authorSeq(0),"message2",true,"url"),
    new Commit("sha", authorSeq(0),"message3",true,"url"),
    new Commit("sha", authorSeq(0),"message4",true,"url"),
    new Commit("sha", authorSeq(0),"message5",true,"url"),
    new Commit("sha", authorSeq(0),"message6",true,"url"),
    new Commit("sha", authorSeq(0),"message7",true,"url"),
    new Commit("sha", authorSeq(0),"message8",true,"url"),
    new Commit("sha", authorSeq(0),"message9",true,"url")
  )
}
