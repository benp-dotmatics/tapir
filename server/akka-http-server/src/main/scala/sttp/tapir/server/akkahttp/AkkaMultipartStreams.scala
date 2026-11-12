package sttp.tapir.server.akkahttp

import akka.NotUsed
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaTypes, Multipart}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{Materializer, SubstreamCancelStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import sttp.model.{Header, Part}
import sttp.tapir.server.model.InvalidMultipartBodyException

import scala.concurrent.{ExecutionContext, Future}

/** Streaming parsing of `multipart/form-data` request bodies, without buffering parts in memory or on disk.
  *
  * Usage:
  * {{{
  * endpoint.post
  *   .in(multipartBoundary)
  *   .in(streamBody(AkkaStreams)(Schema.binary, CodecFormat.MultipartFormData()))
  *   .serverLogic { case (boundary, body) =>
  *     AkkaMultipartStreams.parse(boundary, body).runWith(...)
  *   }
  * }}}
  *
  * The parsed event stream can be further processed using [[singlePart]], [[groupParts]] or [[foreachPart]].
  */
object AkkaMultipartStreams {

  /** An element of a parsed multipart stream: either the start of a new part (`Left`), or a chunk of the current part's body (`Right`). */
  type Event = Either[Part[Unit], ByteString]

  /** Parses a raw `multipart/form-data` byte stream into a flat stream of events:
    *   - `Left(part)`: the start of a new part. The part's body is empty (`Unit`); its name, file name, content type, other disposition
    *     parameters and headers are available.
    *   - `Right(bytes)`: a chunk of the body of the most recently started part.
    *
    * The parts are emitted in the order they appear in the request. Elements are only read from `body` when they are demanded downstream,
    * so a part's bytes can be streamed directly to their destination. Bytes of parts that aren't needed can be ignored (but they're still
    * read from the request).
    *
    * The returned source can be materialized at most once, as it consumes `body`. If the body is malformed, the stream fails with an
    * [[InvalidMultipartBodyException]], which by default is turned into a `400 Bad Request` response, if it propagates from the server
    * logic.
    *
    * @param boundary
    *   The multipart boundary, as read from the `Content-Type` header, e.g. using [[sttp.tapir.Tapir.multipartBoundary]].
    * @param body
    *   The raw request body.
    */
  def parse(boundary: String, body: Source[ByteString, Any])(implicit
      mat: Materializer
  ): Source[Event, NotUsed] = {
    val contentType = ContentType(MediaTypes.`multipart/form-data`.withBoundary(boundary))
    val entity = HttpEntity.IndefiniteLength(contentType, body)
    implicit val ec: ExecutionContext = mat.executionContext

    Source
      .lazyFutureSource(() => Unmarshal(entity).to[Multipart.FormData].map(_.parts))
      .flatMapConcat { part =>
        Source.single(Left(toPart(part))) ++ part.entity.dataBytes.map(Right(_))
      }
      .mapError { case e if isParsingException(e) => InvalidMultipartBodyException(e) }
      .mapMaterializedValue(_ => NotUsed)
  }

  /** Extracts the body of the first part with the given name; the remaining parts are skipped. The stream completes as soon as that part
    * ends: the remainder of the request body isn't read. If there's no such part, the stream fails with an
    * [[InvalidMultipartBodyException]].
    */
  def singlePart(name: String): Flow[Event, ByteString, NotUsed] =
    Flow[Event]
      .dropWhile(e => !e.left.exists(_.name == name))
      .prefixAndTail(1)
      .flatMapConcat {
        case (Seq(_), tail) => tail.takeWhile(_.isRight).collect { case Right(bytes) => bytes }
        case _              => Source.failed(InvalidMultipartBodyException(s"Missing part: $name"))
      }

  /** Groups the events into parts, each with its own byte stream.
    *
    * The parts are emitted one at a time, in the order they appear in the request: the next part is only emitted after the byte stream of
    * the current one has been fully consumed (or cancelled). Hence, the byte stream of each part must be run promptly, and the parts cannot
    * be processed in parallel. Byte streams which aren't run within the configured subscription timeout (by default 5 seconds) fail.
    */
  def groupParts: Flow[Event, (Part[Unit], Source[ByteString, NotUsed]), NotUsed] =
    Flow[Event]
      // drain: when a part's byte stream is cancelled, skip the rest of that part and continue with the next one, instead of completing
      // the whole stream
      .splitWhen(SubstreamCancelStrategy.drain)(_.isLeft)
      .prefixAndTail(1)
      .collect { case (Seq(Left(part)), tail) => part -> tail.collect { case Right(bytes) => bytes } }
      .concatSubstreams

  /** Runs the body of each part with the sink created by `handler`, emitting the parts along with the sinks' results, in the order in which
    * they appear in the request. To skip a part, return `Sink.ignore`.
    *
    * For example, to upload files and read text fields:
    * {{{
    * AkkaMultipartStreams.parse(boundary, body)
    *   .via(AkkaMultipartStreams.foreachPart {
    *     case part if part.fileName.isDefined => uploadToS3(part).mapMaterializedValue(_.map(Some(_)))
    *     case _                               => Sink.ignore.mapMaterializedValue(_.map(_ => None))
    *   })
    *   .runWith(Sink.seq)
    * }}}
    */
  def foreachPart[T](handler: Part[Unit] => Sink[ByteString, Future[T]]): Flow[Event, (Part[Unit], T), NotUsed] =
    Flow
      .fromMaterializer { (mat, _) =>
        implicit val m: Materializer = mat
        implicit val ec: ExecutionContext = mat.executionContext
        groupParts.mapAsync(1) { case (part, bytes) => bytes.runWith(handler(part)).map(part -> _) }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def toPart(part: Multipart.FormData.BodyPart): Part[Unit] =
    Part(
      part.name,
      (),
      otherDispositionParams = part.additionalDispositionParams,
      headers = part.additionalHeaders.map(h => Header(h.name, h.value))
    ).contentType(part.entity.contentType.toString())

  // Akka HTTP reports multipart syntax errors as one of two unrelated `ParsingException` classes: the public one (e.g. for a premature
  // end of the body), and an internal one (e.g. for malformed part headers). The latter is `private[http]`, hence matched by name.
  private def isParsingException(e: Throwable): Boolean =
    e.isInstanceOf[akka.http.scaladsl.model.ParsingException] ||
      e.getClass.getName == "akka.http.impl.engine.parsing.ParsingException"
}
