package reactor.tcp.encoding.json

import reactor.function.Consumer
import reactor.function.Function
import reactor.io.Buffer
import spock.lang.Specification

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

class JsonCodecSpec extends Specification {

	def "JSON can be decoded into a Map"() {
		given: 'A JSON codec'
		JsonCodec<Map<String, Object>, Object> codec = new JsonCodec<Map<String, Object>, Object>(Map);

		when: 'The decoder is passed some JSON'
		Map<String, Object> decoded;
		Function<Buffer, Map<String, Object>> decoder = codec.decoder({ decoded = it} as Consumer<Map<String, Object>>)
		decoder.apply(Buffer.wrap("{\"a\": \"alpha\"}"));

		then: 'The decoded map has the expected entries'
		decoded.size() == 1
		decoded['a'] == 'alpha'
	}

	def "JSON can be decoded into a JsonNode"() {
		given: 'A JSON codec'
		JsonCodec<JsonNode, Object> codec = new JsonCodec<JsonNode, Object>(JsonNode);

		when: 'The decoder is passed some JSON'
		JsonNode decoded
		Function<Buffer, JsonNode> decoder = codec.decoder({ decoded = it} as Consumer<JsonNode>)
		decoder.apply(Buffer.wrap("{\"a\": \"alpha\"}"));

		then: 'The decoded JsonNode is an object node with the expected entries'
		decoded instanceof ObjectNode
		decoded.get('a').textValue() == 'alpha'
	}

}
