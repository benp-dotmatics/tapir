// {cat=Multipart; effects=Future; server=Pekko HTTP}: Streaming a multipart form upload, without buffering the parts

//> using dep com.softwaremill.sttp.tapir::tapir-core:1.14.1
//> using dep com.softwaremill.sttp.tapir::tapir-pekko-http-server:1.14.1
//> using dep org.apache.pekko::pekko-http:1.0.1
//> using dep org.apache.pekko::pekko-stream:1.0.3
//> using dep com.softwaremill.sttp.client4::core:4.0.0-RC3

package sttp.tapir.examples.multipart

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString
import sttp.capabilities.pekko.PekkoStreams
import sttp.client4.*
import sttp.client4.httpclient.HttpClientSyncBackend
import sttp.tapir.*
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoMultipartStreams}

import java.nio.file.{Files, Path}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

@main def streamingMultipartUploadPekkoServer(): Unit =
  implicit val actorSystem: ActorSystem = ActorSystem()
  import actorSystem.dispatcher

  // file parts are streamed to this directory as they arrive; this could be e.g. an S3 bucket instead
  val uploadDir: Path = Files.createTempDirectory("uploads")

  // corresponds to: POST /upload [multipart form data, with any parts]
  //
  // instead of using multipartBody, which reads all parts before the server logic is run, the body is received as a raw
  // stream; multipartBoundary reads the boundary (needed to parse the body) from the Content-Type header
  val uploadEndpoint: PublicEndpoint[(String, PekkoStreams.BinaryStream), Unit, String, PekkoStreams] =
    endpoint.post
      .in("upload")
      .in(multipartBoundary)
      .in(streamBody(PekkoStreams)(Schema.binary, CodecFormat.MultipartFormData()))
      .out(stringBody)

  val uploadRoute: Route = PekkoHttpServerInterpreter().toRoute(uploadEndpoint.serverLogicSuccess { case (boundary, body) =>
    PekkoMultipartStreams
      .parse(boundary, body)
      // each part's body is handled using a separate sink; the parts arrive one after another
      .via(PekkoMultipartStreams.foreachPart[String] {
        case part if part.fileName.isDefined =>
          // streaming the file to disk, without buffering it in memory
          // the file name comes from the client: only using its last segment, so that it can't point outside of uploadDir
          val target = uploadDir.resolve(Path.of(part.fileName.get).getFileName)
          FileIO.toPath(target).mapMaterializedValue(_.map(r => s"${part.name}: stored ${r.count} bytes as ${target.getFileName}"))
        case part =>
          // text fields are small, so they can be read into memory
          Sink.fold[ByteString, ByteString](ByteString.empty)(_ ++ _).mapMaterializedValue(_.map(v => s"${part.name}: ${v.utf8String}"))
      })
      .runWith(Sink.seq)
      .map(_.map(_._2).mkString("\n"))
  })

  // starting the server
  val bindAndCheck: Future[Http.ServerBinding] = Http().newServerAt("localhost", 8080).bindFlow(uploadRoute).map { binding =>
    // testing
    val backend: SyncBackend = HttpClientSyncBackend()
    val result: String = basicRequest
      .response(asStringAlways)
      .post(uri"http://localhost:8080/upload")
      .multipartBody(
        multipart("description", "holiday photos"),
        multipart("photo1", Array.fill[Byte](100000)(1)).fileName("photo1.jpg"),
        multipart("photo2", Array.fill[Byte](200000)(2)).fileName("photo2.jpg")
      )
      .send(backend)
      .body
    println("Got result:\n" + result)

    assert(
      result ==
        """description: holiday photos
          |photo1: stored 100000 bytes as photo1.jpg
          |photo2: stored 200000 bytes as photo2.jpg""".stripMargin
    )
    assert(Files.size(uploadDir.resolve("photo2.jpg")) == 200000)

    binding
  }

  // stopping the server and the actor system, so that the JVM can exit
  val _ = Await.result(bindAndCheck.flatMap(_.terminate(1.minute)).flatMap(_ => actorSystem.terminate()), 1.minute)
