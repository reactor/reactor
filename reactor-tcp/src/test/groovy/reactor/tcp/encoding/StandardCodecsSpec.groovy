package reactor.tcp.encoding

import reactor.fn.Consumer
import reactor.io.Buffer
import reactor.tcp.encoding.syslog.SyslogCodec
import spock.lang.Specification

/**
 * Tests to cover the basic, built-in Codecs.
 * @author Jon Brisbin
 */
class StandardCodecsSpec extends Specification {

	def "StringCodec can encode and decode to Strings"() {
		given: "String data"
		def codec = new StringCodec()
		def data = Buffer.wrap("Hello World!")
		def hello = ""

		when: "the Buffer is decoded"
		hello = codec.decoder(null).apply(data)

		then: "the String was decoded"
		hello == "Hello World!"

		when: "the String is encoded"
		data = codec.encoder().apply(hello)

		then: "the String was encoded"
		data instanceof Buffer
		data.asString() == "Hello World!"
	}

	def "DelimitedCodec can encode and decode delimited lines"() {
		given: "delimited data"
		def codec = new DelimitedCodec<String, String>(StandardCodecs.STRING_CODEC)
		def data = Buffer.wrap("Hello World!\nHello World!\nHello World!\n")
		def hellos = []

		when: "data is decoded"
		def decoder = codec.decoder({ String s ->
			hellos << s
		} as Consumer<String>)
		decoder.apply(data)

		then: "data was decoded"
		hellos == ["Hello World!\n", "Hello World!\n", "Hello World!\n"]

		when: "data is encoded"
		def encoder = codec.encoder()
		def hw1 = encoder.apply("Hello World!")
		def hw2 = encoder.apply("Hello World!")
		def hw3 = encoder.apply("Hello World!")
		def buff = new Buffer().append(hw1, hw2, hw3).flip()

		then: "data was encoded"
		buff.asString() == data.flip().asString()
	}

	def "LengthFieldCodec can encode and decode length-prefixed items"() {
		given: "length-prefixed data"
		def codec = new LengthFieldCodec<String, String>(StandardCodecs.STRING_CODEC)
		def data = new Buffer().append((int) 12).append("Hello World!").flip()

		when: "the data is decoded"
		def len = data.readInt()
		def hello = data.asString()

		then: "the data was decoded"
		len == 12
		hello == "Hello World!"

		when: "the data is encoded"
		data = codec.encoder().apply(hello)

		then: "the data was encoded"
		data.readInt() == 12
		data.asString() == "Hello World!"
	}

	def "SyslogCodec can decode syslog messages"() {
		given: "syslog data"
		def codec = new SyslogCodec()
		def data = Buffer.wrap("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n")
		def host = ""

		when: "data is decoded"
		def msg = codec.decoder(null).apply(data)
		host = msg.host

		then: "data was decoded"
		host == "mymachine"
	}

}
