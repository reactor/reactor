package reactor.io.encoding.kryo

import com.esotericsoftware.kryo.Kryo
import reactor.io.Buffer
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class KryoCodecSpec extends Specification {

	Kryo kryo

	def setup() {
		kryo = new Kryo()
		kryo.register(RichObject)
	}

	def "properly serializes and deserializes objects"() {

		given: "a Kryo codec and a Buffer"
			def codec = new KryoCodec(kryo)
			RichObject obj = new RichObject("first", 0.5f, 100l)
			Buffer buffer

		when: "an objects are serialized"
			buffer = codec.encoder().apply(obj)

		then: "all objects were serialized"
			buffer.remaining() == 78

		when: "an object is deserialized"
			RichObject newObj = codec.decoder(null).apply(buffer)

		then: "the object was deserialized"
			newObj.name == "first"
			newObj.percent == 0.5f
			newObj.total == 100l

	}

	static class RichObject {
		String name
		Float percent
		Long total

		RichObject() {
		}

		RichObject(String name, Float percent, Long total) {
			this.name = name
			this.percent = percent
			this.total = total
		}
	}

}
