import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }
object ClientCreationWithCaseClass extends App {

    object Client {
      def apply(name: String, inboundFeedUrl: String): Client = Client(new ObjectId(), name, inboundFeedUrl);
    }
    case class Client(_id: ObjectId, name: String, inboundFeedUrl: String)

    val codecRegistry = fromRegistries(fromProviders(classOf[Client]), DEFAULT_CODEC_REGISTRY)

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

    val mongoClient: MongoClient = if (args.isEmpty) MongoClient() else MongoClient(args.head)
    val database: MongoDatabase = mongoClient.getDatabase("test").withCodecRegistry(codecRegistry)
    val collection: MongoCollection[Client] = database.getCollection("jobs")
    collection.drop().results()

    // INSERTION
    val client: Client = Client("client1", "url1")

    collection.insertOne(client).results()

    // READING
    collection.find.first().printResults()

    val clients: Seq[Client] = Seq(
      Client("client2", "url2"),
      Client("client3", "url3"),
      Client("client4", "url4")
    )
    collection.insertMany(clients).printResults()


    collection.find().first().printHeadResult()


    collection.find(equal("name", "client3")).first().printHeadResult()

    collection.updateOne(equal("inboundFeedUrl", "url3"), set("inboundFeedUrl", "url33")).printHeadResult("Update Result: ")

    collection.deleteOne(equal("name", "client4")).printHeadResult("Delete Result: ")

    mongoClient.close()


}