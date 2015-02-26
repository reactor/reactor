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

package reactor.io.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.internal.spi.json.JacksonJsonProvider;
import reactor.bus.selector.ObjectSelector;
import reactor.bus.selector.Selector;
import reactor.io.buffer.Buffer;

import java.util.Collection;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class JsonPathSelector extends ObjectSelector<JsonPath> {

	// Only need one of these
	private static ObjectMapper MAPPER = new ObjectMapper();

	private final ObjectMapper  mapper;
	private final Configuration jsonPathConfig;

	public JsonPathSelector(ObjectMapper mapper, String jsonPath, Filter... filters) {
		super(JsonPath.compile(jsonPath, filters));
		this.mapper = mapper;
		this.jsonPathConfig = Configuration.builder().jsonProvider(new JacksonJsonProvider(mapper)).build();
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
		}
		// default
		return !JsonNode.class.isAssignableFrom(type) || ((JsonNode) result).size() > 0;
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
		}
		// default
		return getObject().read(mapper.convertValue(key, Object.class), jsonPathConfig);
	}

}
