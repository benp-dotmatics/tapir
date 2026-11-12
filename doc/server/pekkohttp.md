# Running as a pekko-http server

To expose an endpoint as a [pekko-http](https://pekko.apache.org/docs/pekko-http/current/) server, first add the following 
dependency:

```scala
"com.softwaremill.sttp.tapir" %% "tapir-pekko-http-server" % "@VERSION@"
```

This will transitively pull some Pekko modules. If you want to force
your own Pekko version, use sbt exclusion. Mind the Scala version in artifact name:

```scala
"com.softwaremill.sttp.tapir" %% "tapir-pekko-http-server" % "@VERSION@" exclude("org.apache.pekko", "pekko-stream_2.12")
```

Now import the object:

```scala mdoc:compile-only
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter
```

## Using `toRoute`

The `toRoute` method requires a single, or a list of `ServerEndpoint`s, which can be created by adding 
[server logic](logic.md) to an endpoint.

For example:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter
import scala.concurrent.Future
import org.apache.pekko.http.scaladsl.server.Route
import scala.concurrent.ExecutionContext.Implicits.global

def countCharacters(s: String): Future[Either[Unit, Int]] = 
  Future.successful(Right[Unit, Int](s.length))

val countCharactersEndpoint: PublicEndpoint[String, Unit, Int, Any] = 
  endpoint.in(stringBody).out(plainBody[Int])
  
val countCharactersRoute: Route = 
  PekkoHttpServerInterpreter().toRoute(countCharactersEndpoint.serverLogic(countCharacters))
```

## Combining directives

The tapir-generated `Route` captures from the request only what is described by the endpoint. Combine
with other pekko-http directives to add additional behavior, or get more information from the request.

For example, wrap the tapir-generated route in a metrics route, or nest a security directive in the
tapir-generated directive.

Edge-case endpoints, which require special logic not expressible using tapir, can be implemented directly
using pekko-http. For example:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter
import org.apache.pekko.http.scaladsl.server.*

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Special
def metricsDirective: Directive0 = ???
def specialDirective: Directive1[Special] = ???
val tapirEndpoint: PublicEndpoint[String, Unit, Unit, Any] = endpoint.in(path[String]("input"))

val myRoute: Route = metricsDirective {
  specialDirective { special =>
    PekkoHttpServerInterpreter().toRoute(tapirEndpoint.serverLogic[Future] { input => 
      ??? 
      /* here we can use both `special` and `input` values */
    })
  }
}
```

## Streaming

The pekko-http interpreter accepts streaming bodies of type `Source[ByteString, Any]`, as described by the `PekkoStreams`
capability. Both response bodies and request bodies can be streamed. Usage: `streamBody(PekkoStreams)(schema, format)`.

The capability can be added to the classpath independently of the interpreter through the 
`"com.softwaremill.sttp.shared" %% "pekko"` dependency.

```{note}
By default, pekko-http fails reading request bodies sent using chunked transfer encoding, which contain a chunk larger
than 1MB. When the body is a stream, this results in an internal server error response. The limit can be changed using
the `pekko.http.server.parsing.max-chunk-size` configuration setting.
```

### Streaming multipart forms

Multipart bodies can be processed as they arrive, without buffering the parts in memory or in temporary files (see
[streaming multipart forms](../endpoint/forms.md#streaming-multipart-forms)). `PekkoMultipartStreams.parse` turns the
multipart boundary and the raw request body into a stream of events: `Left(part)` marks the start of each part, followed
by `Right(bytes)` chunks of its body. The following helpers can be used to further process the events:

* `singlePart(name)`: extracts the body of a single part, skipping the others. The remainder of the request isn't read
  once the part ends. If there's no such part, the stream fails with an `InvalidMultipartBodyException`
* `foreachPart(handler)`: runs the body of each part using a `Sink` created by the given handler, emitting the results
* `groupParts`: emits each part along with a source of its body bytes; the source must be run before the next part is
  emitted

For example, to stream each uploaded file to a destination, collecting the remaining parts as text fields:

```scala mdoc:compile-only
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.pekkohttp.PekkoMultipartStreams

import scala.concurrent.{ExecutionContext, Future}

def upload(fileName: String): Sink[ByteString, Future[Done]] = ???

def uploadEndpoint(implicit mat: Materializer, ec: ExecutionContext): ServerEndpoint[PekkoStreams, Future] =
  endpoint.post
    .in("upload")
    .in(multipartBoundary)
    .in(streamBody(PekkoStreams)(Schema.binary, CodecFormat.MultipartFormData()))
    .out(stringBody)
    .serverLogicSuccess[Future] { case (boundary, body) =>
      PekkoMultipartStreams
        .parse(boundary, body)
        .via(PekkoMultipartStreams.foreachPart[String] {
          case p if p.fileName.isDefined =>
            upload(p.fileName.get).mapMaterializedValue(_.map(_ => s"${p.name}: uploaded"))
          case p => 
            Sink.fold[ByteString, ByteString](ByteString.empty)(_ ++ _).mapMaterializedValue(_.map(v => s"${p.name}: ${v.utf8String}"))
        })
        .runWith(Sink.seq)
        .map(_.map(_._2).mkString("\n"))
    }
```

## Web sockets

The interpreter supports web sockets, with pipes of type `Flow[REQ, RESP, Any]`. See [web sockets](../endpoint/websockets.md) 
for more details.

pekko-http does not expose control frames (`Ping`, `Pong` and `Close`), so any setting regarding them are discarded, and
ping/pong frames which are sent explicitly are ignored. [Automatic pings](https://pekko.apache.org/docs/pekko-http/current/server-side/websocket-support.html#automatic-keep-alive-ping-support) can be instead enabled through configuration.

## Server Sent Events

The interpreter supports [SSE (Server Sent Events)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events). 

For example, to define an endpoint that returns event stream:

```scala mdoc:compile-only
import org.apache.pekko.stream.scaladsl.Source
import sttp.model.sse.ServerSentEvent
import sttp.tapir.*
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, serverSentEventsBody}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

val sseEndpoint = endpoint.get.out(serverSentEventsBody)

val routes = PekkoHttpServerInterpreter().toRoute(sseEndpoint.serverLogicSuccess[Future](_ =>
  Future.successful(Source.single(ServerSentEvent(Some("data"), None, None, None)))
))
```

## Configuration

The interpreter can be configured by providing an `PekkoHttpServerOptions` value, see
[server options](options.md) for details.
