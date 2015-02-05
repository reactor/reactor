/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.net.http.model;

import reactor.core.support.LinkedCaseInsensitiveMap;

import java.text.ParseException;
import java.util.*;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class DefaultServerResponseHeaders implements ResponseHeaders {

	private final Map<String, List<String>>
			headers = new LinkedCaseInsensitiveMap<List<String>>(8, Locale.ENGLISH);

	@Override
	public ResponseHeaders add(String name, String value) {
		List<String> values = headers.get(name);
		if (values == null) {
			values = new LinkedList<String>();
			this.headers.put(name, values);
		}
		values.add(value);
		return this;
	}

	@Override
	public ResponseHeaders add(String name, Iterable<String> values) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders addDateHeader(String name, Date value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders clear() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean contains(String name) {
		return this.headers.containsKey(name);
	}

	@Override
	public boolean contains(String name, String value, boolean ignoreCaseValue) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Map.Entry<String, String>> entries() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String get(String name) {
		List<String> values = headers.get(name);
		return values != null ? values.get(0) : null;
	}

	@Override
	public List<String> getAll(String name) {
		return this.headers.get(name);
	}

	@Override
	public long getContentLength() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Date getDate() throws ParseException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Date getDateHeader(String name) throws ParseException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String getHost() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean isEmpty() {
		return this.headers.isEmpty();
	}

	@Override
	public Set<String> names() {
		return this.headers.keySet();
	}

	@Override
	public ResponseHeaders remove(String name) {
		this.headers.remove(name);
		return this;
	}

	@Override
	public ResponseHeaders removeTransferEncodingChunked() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders set(String name, String value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders set(String name, Iterable<String> values) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders contentLength(long length) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders date(Date value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders dateHeader(String name, Date value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders dateHeader(String name, Iterable<Date> values) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders host(String value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders keepAlive(boolean keepAlive) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public ResponseHeaders transferEncodingChunked() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

}
