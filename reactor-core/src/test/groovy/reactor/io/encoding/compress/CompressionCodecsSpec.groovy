package reactor.io.encoding.compress

import reactor.io.Buffer
import spock.lang.Specification

import static reactor.io.encoding.StandardCodecs.PASS_THROUGH_CODEC

/**
 * @author Jon Brisbin
 */
class CompressionCodecsSpec extends Specification {

	GzipCodec<Buffer, Buffer> gzip
	SnappyCodec<Buffer, Buffer> snappy

	def setup() {
		gzip = new GzipCodec<Buffer, Buffer>(PASS_THROUGH_CODEC)
		snappy = new SnappyCodec<Buffer, Buffer>(PASS_THROUGH_CODEC)
	}

	def "compression codecs preserve integrity of data"() {

		given:
			Buffer buffer

		when: "an object is encoded with GZIP"
			buffer = gzip.encoder().apply(Buffer.wrap("Hello World!"))

		then: "the Buffer was encoded and compressed"
			buffer.remaining() == 32

		when: "an object is decoded with GZIP"
			String hw = gzip.decoder(null).apply(buffer).asString()

		then: "the Buffer was decoded and uncompressed"
			hw == "Hello World!"

		when: "an object is encoded with Snappy"
			buffer = snappy.encoder().apply(Buffer.wrap("Hello World!"))

		then: "the Buffer was encoded and compressed"
			buffer.remaining() == 34

		when: "an object is decoded with Snappy"
			hw = snappy.decoder(null).apply(buffer).asString()

		then: "the Buffer was decoded and uncompressed"
			hw == "Hello World!"

	}

}
