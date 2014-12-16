package reactor.io.codec.protobuf

import reactor.io.buffer.Buffer
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class ProtobufCodecSpec extends Specification {

	TestObjects.RichObject obj

	def setup() {
		obj = TestObjects.RichObject.newBuilder().
				setName("first").
				setPercent(0.5f).
				setTotal(100l).
				build()
	}

	def "properly serializes and deserializes objects"() {

		given: "a ProtobufCodec and a Buffer"
			def codec = new ProtobufCodec<TestObjects.RichObject, TestObjects.RichObject>()
			Buffer buffer

		when: "an object is serialized"
			buffer = codec.encoder().apply(obj)

		then: "the object ws serialized"
			buffer.remaining() == 70

		when: "an object is deserialized"
			TestObjects.RichObject newObj = codec.decoder(null).apply(buffer)

		then: "the object was deserialized"
			newObj.name == "first"
			newObj.percent == 0.5f
			newObj.total == 100l

	}

}
