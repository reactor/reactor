/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.selector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import reactor.io.buffer.Buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Jon Brisbin
 */
public class JsonPathSelector extends ObjectSelector<Object, JsonPath> {

	// Only need one of these
	private static ObjectMapper MAPPER = new ObjectMapper();

	private final ObjectMapper  mapper;
	private final Configuration jsonPathConfig;

	public JsonPathSelector(ObjectMapper mapper, String jsonPath, Filter... filters) {
		super(JsonPath.compile(jsonPath, filters));
		this.mapper = mapper;
		this.jsonPathConfig = Configuration.builder().jsonProvider(new Jackson2JsonProvider(mapper)).build();
	}

	public JsonPathSelector(String jsonPath, Filter... filters) {
		this(MAPPER, jsonPath, filters);
	}

	public static Selector J(String jsonPath, Filter... filters) {
		return jsonPathSelector(jsonPath, filters);
	}

	public static Selector jsonPathSelector(String jsonPath, Filter... filters) {
		return new JsonPathSelector(jsonPath, filters);
	}

	@Override
	public boolean matches(Object key) {
		if (null == key) {
			return false;
		}

		Object result = read(key);
		if (null == result) {
			return false;
		}
		Class<?> type = result.getClass();
		if (Collection.class.isAssignableFrom(type)) {
			return ((Collection) result).size() > 0;
		} else if (Map.class.isAssignableFrom(type)) {
			return ((Map) result).size() > 0;
		} else if (JsonNode.class.isAssignableFrom(type)) {
			return ((JsonNode) result).size() > 0;
		} else {
			return true;
		}
	}

	private Object read(Object key) {
		Class<?> type = key.getClass();
		if (type == String.class) {
			return getObject().read((String) key, jsonPathConfig);
		} else if (type == byte[].class) {
			return getObject().read(Buffer.wrap((byte[]) key).asString(), jsonPathConfig);
		} else if (type == Buffer.class) {
			return getObject().read(((Buffer) key).asString(), jsonPathConfig);
		} else if (JsonNode.class.isAssignableFrom(type)) {
			return getObject().read(key, jsonPathConfig);
		} else {
			return getObject().read(mapper.convertValue(key, Object.class), jsonPathConfig);
		}
	}

	@SuppressWarnings("unchecked")
	private static class Jackson2JsonProvider implements JsonProvider, MappingProvider {
		private final ObjectMapper mapper;

		private Jackson2JsonProvider(ObjectMapper mapper) {
			this.mapper = mapper;
		}

		@Override
		public Object parse(String json) throws InvalidJsonException {
			try {
				return mapper.readValue(json, Object.class);
			} catch (IOException e) {
				throw new InvalidJsonException(e.getMessage(), e);
			}
		}

		@Override
		public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
			try {
				return mapper.readValue(new InputStreamReader(jsonStream, charset), Object.class);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public String toJson(Object obj) {
			try {
				return mapper.writeValueAsString(obj);
			} catch (JsonProcessingException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		@Override
		public Object createMap() {
			return new HashMap();
		}

		@Override
		public Iterable createArray() {
			return new ArrayList();
		}


		@Override
		public Object getArrayIndex(Object obj, int idx) {
			return getProperty(obj, idx);
		}

		@Override
		public Object getArrayIndex(Object obj, int idx, boolean unwrap) {
			Object o = getProperty(obj, idx);
			if (o instanceof JsonNode && unwrap) {
				return unwrap((JsonNode) o);
			} else {
				return o;
			}
		}

		@Override
		public void setArrayIndex(Object array, int idx, Object newValue) {
			setProperty(array, idx, newValue);
		}

		@Override
		public Object getMapValue(Object obj, String key) {
			return getProperty(obj, key);
		}

		@Override
		public void removeProperty(Object obj, Object key) {
			setProperty(obj, key, null);
		}

		@Override
		public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
			return mapper.convertValue(source, targetType);
		}

		@Override
		public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration) {
			if (targetType.getType() instanceof Class) {
				return mapper.convertValue(source, (Class<T>) targetType.getType());
			} else {
				throw new IllegalArgumentException("Cannot convert to " + targetType);
			}
		}

		@Override
		public boolean isArray(Object obj) {
			return (obj instanceof List || obj instanceof ArrayNode);
		}

		@Override
		public boolean isMap(Object obj) {
			return (obj instanceof Map || obj instanceof ObjectNode);
		}

		@Override
		public int length(Object obj) {
			if (obj instanceof List) {
				return ((List) obj).size();
			} else if (obj instanceof Map) {
				return ((Map) obj).size();
			} else if (obj instanceof ArrayNode) {
				return ((ArrayNode) obj).size();
			} else if (obj instanceof ObjectNode) {
				return ((ObjectNode) obj).size();
			} else {
				return length(mapper.convertValue(obj, JsonNode.class));
			}
		}

		@Override
		public Iterable<Object> toIterable(Object obj) {
			if (obj instanceof List || obj instanceof JsonNode) {
				return (Iterable<Object>) obj;
			} else if (obj instanceof Map) {
				return ((Map) obj).values();
			} else {
				return toIterable(mapper.convertValue(obj, JsonNode.class));
			}
		}

		@Override
		public Collection<String> getPropertyKeys(Object obj) {
			if (obj instanceof Map) {
				return ((Map) obj).keySet();
			} else if (obj instanceof List) {
				List l = (List) obj;
				List<String> keys = new ArrayList<String>(l.size());
				for (Object o : l) {
					keys.add(String.valueOf(o));
				}
				return keys;
			} else if (obj instanceof ObjectNode) {
				ObjectNode node = (ObjectNode) obj;
				List<String> keys = new ArrayList<String>(node.size());
				Iterator<String> iter = node.fieldNames();
				while (iter.hasNext()) {
					keys.add(iter.next());
				}
				return keys;
			} else if (obj instanceof ArrayNode) {
				ArrayNode node = (ArrayNode) obj;
				List<String> keys = new ArrayList<String>(node.size());
				int len = node.size();
				for (int i = 0; i < len; i++) {
					keys.add(String.valueOf(node.get(i)));
				}
				return keys;
			} else {
				return getPropertyKeys(mapper.convertValue(obj, JsonNode.class));
			}
		}

		@Override
		public void setProperty(Object obj, Object key, Object value) {
			if (obj instanceof Map) {
				((Map) obj).put(key, value);
			} else if (obj instanceof List) {
				int idx = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
				((List) obj).add(idx, value);
			} else if (obj instanceof ObjectNode) {
				((ObjectNode) obj).set(key.toString(), (JsonNode) value);
			} else if (obj instanceof ArrayNode) {
				int idx = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
				((ArrayNode) obj).set(idx, (JsonNode) value);
			} else {
				setProperty(mapper.convertValue(obj, JsonNode.class), key, value);
			}
		}

		@Override
		public Object unwrap(Object object) {
			if (object instanceof JsonNode) {
				return unwrap((JsonNode) object);
			} else {
				return object;
			}
		}

		private Object getProperty(Object obj, Object key) {
			if (obj instanceof Map) {
				return ((Map) obj).get(key);
			} else if (obj instanceof List) {
				int idx = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
				return ((List) obj).get(idx);
			} else if (obj instanceof ObjectNode) {
				return unwrap(((ObjectNode) obj).get(key.toString()));
			} else if (obj instanceof ArrayNode) {
				int idx = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
				return unwrap(((ArrayNode) obj).get(idx));
			} else {
				return getProperty(mapper.convertValue(obj, JsonNode.class), key);
			}
		}

		private Object unwrap(JsonNode node) {
			if (node.isValueNode()) {
				if (node.isNumber()) {
					return node.numberValue();
				} else if (node.isTextual()) {
					return node.asText();
				} else {
					return node;
				}
			} else {
				return node;
			}
		}
	}

}
