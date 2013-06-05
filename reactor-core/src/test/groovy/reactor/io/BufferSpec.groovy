package reactor.io

import spock.lang.Specification

import java.nio.ByteBuffer

/**
 * @author Jon Brisbin
 */
class BufferSpec extends Specification {

	def "A Buffer reads and writes bytes"() {
		given: "an empty Buffer"
		def buff = new Buffer()

		when: "a byte is appended"
		buff.append((byte) 1)

		then: "the byte was added"
		buff.position() == 1
		buff.flip().read() == 1
	}

	def "A Buffer reads and writes integers"() {
		given: "an empty Buffer"
		def buff = new Buffer()

		when: "an int is appended"
		buff.append(1)

		then: "the int was added"
		buff.position() == 4
		buff.flip().readInt() == 1
	}

	def "A Buffer reads and writes longs"() {
		given: "an empty Buffer"
		def buff = new Buffer()

		when: "a long is appended"
		buff.append(1L)

		then: "the long was added"
		buff.position() == 8
		buff.flip().readLong() == 1L
	}

	def "A Buffer reads and writes byte[]"() {
		given: "an empty Buffer"
		def buff = new Buffer()

		when: "a byte[] is appended and read"
		buff.append("Hello World!".getBytes())

		then: "the byte[]s were added"
		buff.position() == 12

		when: "the byte[]s are read"
		def bytes = new byte[12]
		buff.flip().read(bytes)

		then: "the bytes were read"
		bytes[11] == '!'
	}

	def "A Buffer reads and writes Strings"() {
		given: "an empty Buffer"
		def buff = new Buffer()

		when: "a String is appended"
		buff.append("Hello World!")

		then: "the String was added"
		buff.position() == 12
		buff.flip().asString() == "Hello World!"
	}

	def "A Buffer reads and writes ByteBuffers"() {
		given: "an empty Buffer and full ByteBuffer"
		def buff = new Buffer()
		def bb = ByteBuffer.wrap("Hello World!".getBytes())

		when: "a ByteBuffer is appended"
		buff.append(bb)

		then: "the ByteBuffer was added"
		buff.position() == 12
		buff.flip().asString() == "Hello World!"
	}

	def "A Buffer reads and writes Buffers"() {
		given: "an empty Buffer and a full Buffer"
		def buff = new Buffer()
		def fullBuff = Buffer.wrap("Hello World!")

		when: "a Buffer is appended"
		buff.append(fullBuff)

		then: "the Buffer was added"
		buff.position() == 12
		buff.flip().asString() == "Hello World!"
	}

	def "A Buffer prepends Strings to an existing Buffer"() {
		given: "an full Buffer"
		def buff = Buffer.wrap("World!", false)

		when: "a String is prepended"
		buff.prepend("Hello ")

		then: "the String was prepended"
		buff.asString() == "Hello World!"
	}

	def "A Buffer can be sliced into segments"() {
		given: "a syslog message, buffered"
		def buff = Buffer.wrap("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n")

		when: "positions are assigned and the buffer is sliced"
		def positions = [1, 3, 4, 19, 20, 29, 30]
		def slices = buff.slice(positions)

		then: "the buffer is sliced"
		slices[0].asString() == "34"
		slices[1].asString() == "Oct 11 22:14:15"
		slices[2].asString() == "mymachine"
		slices[3].asString() == "su: 'su root' failed for lonvick on /dev/pts/8\n"
	}

}
