package sttp.tapir.server.pekkohttp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.server.{Directives, RequestContext}
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers._
import sttp.capabilities.pekko.PekkoStreams
import sttp.client4._
import sttp.client4.pekkohttp.PekkoHttpBackend
import sttp.model.sse.ServerSentEvent
import sttp.model.Header
import sttp.model.MediaType
import sttp.model.StatusCode
import sttp.monad.FutureMonad
import sttp.monad.syntax._
import sttp.tapir._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.interceptor._
import sttp.tapir.server.model.MaxContentLength
import sttp.tapir.server.interceptor.metrics.MetricsRequestInterceptor
import sttp.tapir.server.metrics.{EndpointMetric, Metric}
import sttp.tapir.server.tests._
import sttp.tapir.tests.{Test, TestSuite}

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class PekkoHttpServerTest extends TestSuite with EitherValues {
  private def randomUUID = Some(UUID.randomUUID().toString)
  private val sse1 = ServerSentEvent(randomUUID, randomUUID, randomUUID, Some(Random.nextInt(200)))
  private val sse2 = ServerSentEvent(randomUUID, randomUUID, randomUUID, Some(Random.nextInt(200)))

  def actorSystemResource: Resource[IO, ActorSystem] =
    Resource.make(IO.delay(ActorSystem()))(actorSystem => IO.fromFuture(IO.delay(actorSystem.terminate())).void)

  override def tests: Resource[IO, List[Test]] = backendResource.flatMap { backend =>
    actorSystemResource.map { implicit actorSystem =>
      implicit val m: FutureMonad = new FutureMonad()(using actorSystem.dispatcher)

      val interpreter = new PekkoHttpTestServerInterpreter()(using actorSystem)
      val createServerTest = new DefaultCreateServerTest(backend, interpreter)

      def additionalTests(): List[Test] = List(
        Test("endpoint nested in a path directive") {
          val e = endpoint.get.in("test" `and` "directive").out(stringBody).serverLogic(_ => ("ok".asRight[Unit]).unit)
          val route = Directives.pathPrefix("api")(PekkoHttpServerInterpreter().toRoute(e))
          interpreter
            .server(route)
            .use { port =>
              basicRequest.get(uri"http://localhost:$port/api/test/directive").send(backend).map(_.body shouldBe Right("ok"))
            }
            .unsafeToFuture()
        },
        Test("Send and receive SSE") {
          implicit val ec = actorSystem.dispatcher
          val e = endpoint.get
            .in("sse")
            .out(serverSentEventsBody)
            .serverLogicSuccess[Future](_ => {
              Future.successful(Source(List(sse1, sse2)))
            })
          val route = PekkoHttpServerInterpreter().toRoute(e)
          // the default 1s response-entity-subscription-timeout can be exceeded on a loaded CI runner before the
          // SSE-parsing stream is materialized, failing the entity stream; hence raising the timeout
          val clientBackend = PekkoHttpBackend.usingActorSystem(
            actorSystem,
            customConnectionPoolSettings = Some(ConnectionPoolSettings(actorSystem).withResponseEntitySubscriptionTimeout(1.minute))
          )
          interpreter
            .server(route)
            .use { port =>
              IO.fromFuture {
                IO(
                  basicRequest
                    .get(uri"http://localhost:$port/sse")
                    .response(
                      asStreamUnsafe(PekkoStreams).map(errorOrStream =>
                        errorOrStream.map(stream =>
                          PekkoServerSentEvents.parseBytesToSSE(stream).runFold(List.empty[ServerSentEvent])((acc, sse) => acc :+ sse)
                        )
                      )
                    )
                    .send(clientBackend)
                    .flatMap(_.body.value.map(sse => sse shouldBe List(sse1, sse2)))
                    .andThen { case _ => clientBackend.close() }
                )
              }
            }
            .unsafeToFuture()
        },
        Test("replace body using a request interceptor") {
          val e = endpoint.post.in(stringBody).out(stringBody).serverLogicSuccess[Future](body => Future.successful(body))

          val route = PekkoHttpServerInterpreter(
            PekkoHttpServerOptions.customiseInterceptors
              .prependInterceptor(RequestInterceptor.transformServerRequest { request =>
                val underlying = request.underlying.asInstanceOf[RequestContext]
                val changedUnderlying = underlying.withRequest(underlying.request.withEntity(HttpEntity("replaced")))
                Future.successful(request.withUnderlying(changedUnderlying))
              })
              .options
          ).toRoute(e)

          interpreter
            .server(route)
            .use { port =>
              basicRequest.post(uri"http://localhost:$port").body("test123").send(backend).map(_.body shouldBe Right("replaced"))
            }
            .unsafeToFuture()
        },
        Test("execute metrics interceptors for empty body and json content type") {
          val e = endpoint.post
            .in(stringBody)
            .out(stringBody)
            .out(header(Header.contentType(MediaType.ApplicationJson)))
            .serverLogicSuccess[Future](body => Future.successful(body))

          class DummyMetric {
            val onRequestCnt = new AtomicInteger(0)
            val onEndpointRequestCnt = new AtomicInteger(0)
            val onResponseHeadersCnt = new AtomicInteger(0)
            val onResponseBodyCnt = new AtomicInteger(0)
          }
          val metric = new DummyMetric()
          val customMetrics: Metric[Future, DummyMetric] =
            Metric(
              metric = metric,
              onRequest = (_, metric, me) =>
                me.eval {
                  metric.onRequestCnt.incrementAndGet()
                  EndpointMetric(
                    onEndpointRequest = Some((_) => me.eval(metric.onEndpointRequestCnt.incrementAndGet())),
                    onResponseHeaders = Some((_, _) => me.eval(metric.onResponseHeadersCnt.incrementAndGet())),
                    onResponseBody = Some((_, _) => me.eval(metric.onResponseBodyCnt.incrementAndGet())),
                    onException = None
                  )
                }
            )
          val route = PekkoHttpServerInterpreter(
            PekkoHttpServerOptions.customiseInterceptors
              .metricsInterceptor(new MetricsRequestInterceptor[Future](List(customMetrics), Seq.empty))
              .options
          ).toRoute(e)

          interpreter
            .server(route)
            .use { port =>
              basicRequest.post(uri"http://localhost:$port").body("").send(backend).map { response =>
                response.body shouldBe Right("")
                metric.onRequestCnt.get() shouldBe 1
                metric.onEndpointRequestCnt.get() shouldBe 1
                metric.onResponseHeadersCnt.get() shouldBe 1
                metric.onResponseBodyCnt.get() shouldBe 1
              }
            }
            .unsafeToFuture()
        },
        Test("handle status codes >=600") {
          val e = endpoint.get.in("custom_code").errorOut(statusCode).serverLogic(_ => (StatusCode(800).asLeft[Unit]).unit)
          val route = Directives.pathPrefix("api")(PekkoHttpServerInterpreter().toRoute(e))
          interpreter
            .server(route)
            .use { port =>
              basicRequest.get(uri"http://localhost:$port/api/custom_code").send(backend).map(_.code shouldBe StatusCode(800))
            }
            .unsafeToFuture()
        },
        Test("Server reads remote address and secure from connectionInfo") {
          val e = endpoint.get
            .in("test")
            .in(extractFromRequest(req => req.connectionInfo))
            .out(stringBody)
            .serverLogic { connectionInfo =>
              val remote = connectionInfo.remote
              val secure = connectionInfo.secure
              s"$remote $secure".asRight[Unit].unit
            }
          val route = Directives.pathPrefix("api")(PekkoHttpServerInterpreter().toRoute(e))
          interpreter
            .server(route)
            .use { port =>
              basicRequest
                .get(uri"http://localhost:$port/api/test")
                .send(backend)
                .map(_.body.value should fullyMatch regex """Some\(/127\.0\.0\.1:\d+\) Some\(false\)""")
            }
            .unsafeToFuture()
        },
        Test("extractFromRequest(_.uri) returns full URI when nested in path directive") {
          // Given: an endpoint that extracts the URI from the request
          val e = endpoint.get
            .in("test" / "path")
            .in(extractFromRequest(_.uri))
            .out(stringBody)
            .serverLogic { requestUri =>
              requestUri.toString.asRight[Unit].unit
            }

          // When: the route is nested inside a pathPrefix directive
          val route = Directives.pathPrefix("api")(PekkoHttpServerInterpreter().toRoute(e))

          interpreter
            .server(route)
            .use { port =>
              // Then: the extracted URI should contain the full path including the prefix
              basicRequest
                .get(uri"http://localhost:$port/api/test/path?query=value")
                .send(backend)
                .map { response =>
                  response.body.value should include("/api/test/path")
                  response.body.value should include("query=value")
                }
            }
            .unsafeToFuture()
        }
      )
      def multipartStreamingTests(): List[Test] = {

        // summarises each part as name[fileName]:contentType=byteCount
        val uploadEndpoint = endpoint.post
          .in("upload")
          .in(multipartBoundary)
          .in(streamBody(PekkoStreams)(Schema.binary, CodecFormat.MultipartFormData()))
          .out(stringBody)

        def summarise(boundary: String, body: PekkoStreams.BinaryStream): Future[String] =
          PekkoMultipartStreams
            .parse(boundary, body)
            .runFold(Vector.empty[(String, Long)]) {
              case (acc, Left(part)) =>
                acc :+ (s"${part.name}${part.fileName.fold("")(f => s"[$f]")}:${part.contentType.getOrElse("")}" -> 0L)
              case (acc, Right(bytes)) => acc.init :+ (acc.last._1 -> (acc.last._2 + bytes.size))
            }
            .map(_.map { case (p, size) => s"$p=$size" }.mkString(";"))

        def serve(e: ServerEndpoint[PekkoStreams, Future])(test: Int => IO[org.scalatest.Assertion]): Future[org.scalatest.Assertion] =
          interpreter.server(PekkoHttpServerInterpreter().toRoute(e)).use(test).unsafeToFuture()

        val summariseEndpoint = uploadEndpoint.serverLogicSuccess[Future] { case (boundary, body) => summarise(boundary, body) }

        List(
          Test("streaming multipart: parse a multipart body sent by a client") {
            // < 1MB: the test client sends the body as a single HTTP chunk, and pekko-http limits chunks to 1MB by default
            val fileSize = 512 * 1024
            serve(summariseEndpoint) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .multipartBody(
                  multipart("a", "hello"),
                  multipart("f", Array.fill[Byte](fileSize)('x'.toByte)).fileName("big.bin").contentType(MediaType.ApplicationOctetStream)
                )
                .send(backend)
                .map { r =>
                  r.code shouldBe StatusCode.Ok
                  r.body shouldBe Right(s"a:text/plain; charset=UTF-8=5;f[big.bin]:application/octet-stream=$fileSize")
                }
            }
          },
          Test("streaming multipart: extract a single part") {
            val e = uploadEndpoint.serverLogicSuccess[Future] { case (boundary, body) =>
              PekkoMultipartStreams.parse(boundary, body).via(PekkoMultipartStreams.singlePart("f")).runFold(0L)(_ + _.size).map(_.toString)
            }
            serve(e) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .multipartBody(
                  multipart("a", "hello"),
                  multipart("f", Array.fill[Byte](1000)('x'.toByte)).fileName("f.bin"),
                  multipart("b", "x")
                )
                .send(backend)
                .map(_.body shouldBe Right("1000"))
            }
          },
          Test("streaming multipart: 400 when a required part is missing") {
            val e = uploadEndpoint.serverLogicSuccess[Future] { case (boundary, body) =>
              PekkoMultipartStreams.parse(boundary, body).via(PekkoMultipartStreams.singlePart("f")).runFold(0L)(_ + _.size).map(_.toString)
            }
            serve(e) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .multipartBody(multipart("a", "hello"))
                .send(backend)
                .map(_.code shouldBe StatusCode.BadRequest)
            }
          },
          Test("streaming multipart: 400 when the boundary is missing") {
            serve(summariseEndpoint) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .body("--x\r\n")
                .contentType("multipart/form-data")
                .send(backend)
                .map(_.code shouldBe StatusCode.BadRequest)
            }
          },
          Test("streaming multipart: 415 when the content type isn't multipart/form-data") {
            serve(summariseEndpoint) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .body("hello")
                .contentType(MediaType.TextPlain)
                .send(backend)
                .map(_.code shouldBe StatusCode.UnsupportedMediaType)
            }
          },
          Test("streaming multipart: 400 when the body is malformed") {
            serve(summariseEndpoint) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .body("--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nhello, cut off here")
                .contentType("multipart/form-data; boundary=b")
                .send(backend)
                .map(_.code shouldBe StatusCode.BadRequest)
            }
          },
          Test("streaming multipart: 413 when the body exceeds the max content length") {
            val e = uploadEndpoint
              .attribute(AttributeKey[MaxContentLength], MaxContentLength(1024L))
              .serverLogicSuccess[Future] { case (boundary, body) => summarise(boundary, body) }
            serve(e) { port =>
              basicRequest
                .post(uri"http://localhost:$port/upload")
                .multipartBody(multipart("f", Array.fill[Byte](16 * 1024)('x'.toByte)).fileName("big.bin"))
                .send(backend)
                .map(_.code shouldBe StatusCode.PayloadTooLarge)
            }
          }
        )
      }

      def drainPekko(stream: PekkoStreams.BinaryStream): Future[Unit] =
        stream.runWith(Sink.ignore).map(_ => ())

      new AllServerTests(createServerTest, interpreter, backend, staticContent = false).tests() ++
        // pekko-http 1.4.0 drops Content-Length from HEAD responses: https://github.com/apache/pekko-http/issues/1236
        new ServerFilesTests(interpreter, backend, supportContentLengthInHeadRequests = false).tests() ++
        // for a streamed body, sends the response before reading it, without 100 Continue
        new ServerStreamingTests(createServerTest, expectContinueSupported = false).tests(PekkoStreams)(drainPekko) ++
        new ServerWebSocketTests(
          createServerTest,
          PekkoStreams,
          autoPing = false,
          handlePong = false,
          decodeCloseRequests = false
        ) {
          override def functionToPipe[A, B](f: A => B): streams.Pipe[A, B] = Flow.fromFunction(f)
          override def emptyPipe[A, B]: Flow[A, B, Any] = Flow.fromSinkAndSource(Sink.ignore, Source.empty)
        }.tests() ++
        additionalTests() ++
        multipartStreamingTests()
    }
  }
}
