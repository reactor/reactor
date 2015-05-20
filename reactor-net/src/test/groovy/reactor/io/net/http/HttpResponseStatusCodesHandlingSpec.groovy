package reactor.io.net.http

import reactor.Environment
import reactor.io.codec.StandardCodecs
import reactor.io.net.NetStreams
import reactor.rx.Streams
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * @author Anatoly Kadyshev
 */
public class HttpResponseStatusCodesHandlingSpec extends Specification {

    Environment env

    def setup() {
        env = Environment.initializeIfEmpty().assignErrorJournal()
    }

    def "http status code 404 is handled by the client"() {
        given: "a simple HttpServer"
            def server = NetStreams.httpServer {
                it.codec(StandardCodecs.STRING_CODEC).listen(0).dispatcher(Environment.sharedDispatcher())
            }

        when: "the server is prepared"
            server.post('/test') { HttpChannel<String, String> req ->
                req.writeWith(
                        req.log('server-received')
                )
            }

        then: "the server was started"
            server?.start()?.awaitSuccess(5, TimeUnit.SECONDS)

        when: "a request with unsupported URI is sent onto the server"
            def client = NetStreams.httpClient {
                it.codec(StandardCodecs.STRING_CODEC).connect("localhost", server.listenAddress.port)
            }

            def replyReceived = ""
            def content = client.get('/unsupportedURI') { HttpChannel<String,String> req ->
                //prepare content-type
                req.header('Content-Type', 'text/plain')

                //return a producing stream to send some data along the request
                req.writeWith(
                    Streams
                            .just("Hello")
                            .log('client-send')
                )
            }
            .flatMap { replies ->
                //successful request, listen for replies
                replies
                        .log('client-received')
                        .consume { s ->
                            replyReceived = s
                        }
            }
            .onError {
                //something failed during the request or the reply processing
                println "Failed requesting server: $it"
            }

        then: "exception is thrown with a message and no reply received"
            def exceptionMessage = ""

            try {
                content.await();
            } catch (RuntimeException ex) {
                exceptionMessage = ex.getCause().getMessage();
            }

            exceptionMessage == "HTTP request failed with code: 404"
            replyReceived == ""

        cleanup: "the client/server where stopped"
            client?.shutdown()?.onComplete { server.shutdown() }?.awaitSuccess(5, TimeUnit.SECONDS)
    }
}
