import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections
import org.mongodb.scala.model.Updates._

object CrudScalaMongo extends App {

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }

  val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017/")
  val database: MongoDatabase = mongoClient.getDatabase("test")
  val collection: MongoCollection[Document] =database.getCollection("jobs");

  //val doc: Document = Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database","count" -> 1, "info" -> Document("x" -> 203, "y"-> 102))
  //DELETION
  //collection.deleteOne(doc).results();    //Using the results() implicit we block until the observer is completed
  //INSERTION
  //collection.insertOne(doc).results();

  println("LIST OF JOBS AVAILABLE IN HYDERABAD CITY")
  val jobsInHyderbad = collection.find(equal("city", "Hyderabad")).printResults()
  println("LIST OF JOBS AVIALABLE IN LONDON CITY")
  val jobsInLondon = collection.find(equal("city","London")).printHeadResult()
  println("LIST OF JOBS AVAILABLE IN REDWOOD CITY")
  val jobsInRedwood = collection.find(equal("city", "Redwood City")).printResults()
  println("JOBS WHERE CITY IS REDWOOD AND TITLE IS HR MANAGER")
  val jobsInRedwoodTitleHR = collection.find(and(equal("city","Redwood City"),
    equal("title", "HR MANAGER"))).printResults()
  println("JOBS WHERE CITY IS HYDERABAD AND JOB IS UX DESIGNER")
  val jobsInHyderabadUX = collection.find(and(equal("city","Hyderabad"),
    equal("title", "UX DESIGNER"))).printResults()


  //INSERTION
  val document: Document = Document("_id" -> 1000, "x" -> 2000)
  val insertObservable: Observable[Completed] = collection.insertOne(document)

  insertObservable.subscribe(new Observer[Completed] {
    override def onNext(result: Completed): Unit = println(s"onNext: $result")
    override def onError(e: Throwable): Unit = println(s"onError: $e")
    override def onComplete(): Unit = println("onComplete")
  })


  //  UPDATION
  //  collection.updateOne(equal("_id", 188), set("_id", 110)).printHeadResult("Update Result: ")

  //    DELETION
  //  collection.deleteOne(equal("_id", 1)).printHeadResult("Delete Result: ")



}
