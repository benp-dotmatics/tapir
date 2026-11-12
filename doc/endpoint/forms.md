# Forms

## URL-encoded forms

An URL-encoded form input/output can be specified in two ways. First, it is possible to map all form fields as a
`Seq[(String, String)]`, or `Map[String, String]` (which is more convenient if fields can't have multiple values):

```scala mdoc:compile-only
import sttp.tapir.*

formBody[Seq[(String, String)]]: EndpointIO.Body[String, Seq[(String, String)]]
formBody[Map[String, String]]: EndpointIO.Body[String, Map[String, String]]
```

Second, form data can be mapped to a case class. The codec for the case class is automatically derived using a macro at 
compile-time. The fields of the case class should have types, for which there is a plain text codec. For example:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.tapir.generic.auto.*

case class RegistrationForm(name: String, age: Int, news: Boolean, city: Option[String])

formBody[RegistrationForm]: EndpointIO.Body[String, RegistrationForm]
```

Each form-field is named the same as the case-class-field. The names can be transformed to snake or kebab case by 
providing an implicit `tapir.generic.Configuraton`, or customised using the `@encodedName` annotation. 

## Multipart forms

Similarly as above, multipart form input/outputs can be specified in two ways. To map to all parts of a multipart body,
use:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.model.Part

multipartBody: EndpointIO.Body[Seq[RawPart], Seq[Part[Array[Byte]]]]
```

`Part` is a case class containing the `name` of the part, disposition parameters, headers, and the body. The bodies 
will be mapped as byte arrays (`Array[Byte]`). Custom multipart codecs can be defined with the `Codec.multipartCodec`
method, and then used with `multipartBody[T]`.

As with URL-encoded forms, multipart bodies can be mapped directly to case classes, however without the restriction
on codecs for individual fields. Given a field of type `T`, first a plain text codec is looked up, and if one isn't
found, any codec for any media type (e.g. JSON) is searched for.

Each part is named the same as the case-class-field. The names can be transformed to snake or kebab case by 
providing an implicit `sttp.tapir.generic.Configuraton`, or customised using the `@encodedName` annotation. 
 
Additionally, the case class to which the multipart body is mapped can contain both normal fields, and fields of type 
`Part[T]`. This is useful, if part metadata (e.g. the filename) is relevant.

For example:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.model.Part
import java.io.File
import sttp.tapir.generic.auto.*

case class RegistrationForm(userData: User, photo: Part[File], news: Boolean)
case class User(email: String)

multipartBody[RegistrationForm]: EndpointIO.Body[Seq[RawPart], RegistrationForm]
```

The fields can also be wrapped into `Option[T]` or `Option[Part[T]]` if the part is not required.
If there can be none or multiple parts for the same name, the fields can be wrapped into `List[T]` or `List[Part[T]]`
or any other container `C` for which exists a codec `List[T] => C[T]`

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.model.Part
import java.io.File
import sttp.tapir.generic.auto.*

case class RegistrationForm(userData: Option[User], photos: List[Part[File]], news: Option[Part[Boolean]])
case class User(email: String)

multipartBody[RegistrationForm]: EndpointIO.Body[Seq[RawPart], RegistrationForm]
```

```{warning}
When receiving a multipart request in a Tapir server, any files created to store multipart parts will be removed after 
the request processing completes (regardless of the outcome - HTTP success, HTTP failure or exception).
```

## Streaming multipart forms

When using `multipartBody`, all parts are read (into memory or temporary files) before the server logic is invoked. If
the parts are large, and should be processed as they arrive - e.g. uploaded directly to an object store - the multipart
body can instead be received as a raw [stream](streaming.md), and parsed using a streams-specific multipart parser.

To do that, combine the `multipartBoundary` input, which reads the boundary from the `Content-Type` header, with a
streaming body:

```scala mdoc:compile-only
import sttp.tapir.*
import sttp.capabilities.pekko.PekkoStreams

endpoint.post
  .in("upload")
  .in(multipartBoundary)
  .in(streamBody(PekkoStreams)(Schema.binary, CodecFormat.MultipartFormData()))
// : Endpoint[Unit, (String, Source[ByteString, Any]), Unit, Unit, PekkoStreams]
```

If the request's content type isn't `multipart/form-data`, a `415 Unsupported Media Type` response is returned; if the
boundary is missing, the response is a `400 Bad Request`. The schema given to `streamBody` is only used for documentation,
and can describe the expected parts, e.g. `Schema.derived[MyForm]`.

The boundary and raw stream are then passed to a multipart parser, which turns them into a flat stream of events: the
start of each part, with its name, file name, content type and headers (`Left(part: Part[Unit])`), followed by chunks of
the part's body (`Right(bytes)`). Parts are emitted in the order in which they appear in the request, and the request body
is read only as fast as the events are consumed.

Streaming multipart parsing is currently supported by the following server interpreters:

* pekko-http: see [`PekkoMultipartStreams`](../server/pekkohttp.md#streaming-multipart-forms)
* akka-http: see [`AkkaMultipartStreams`](../server/akkahttp.md#streaming-multipart-forms)

```{note}
Since the parts arrive one after another, the body of a part has to be consumed (or skipped) before the next part
becomes available. Hence, parts cannot be processed in parallel, unless they're buffered. 

As the body is parsed by the server logic, any parsing errors happen there as well. These are reported as an
`InvalidMultipartBodyException`, which, if it propagates, is by default handled by responding with a `400 Bad Request`.
```

## Next

Read on about [security](security.md).
