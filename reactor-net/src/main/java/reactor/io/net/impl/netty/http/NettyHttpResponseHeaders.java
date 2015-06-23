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

package reactor.io.net.impl.netty.http;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import reactor.io.net.http.model.ResponseHeaders;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyHttpResponseHeaders implements ResponseHeaders {

	private final HttpResponse nettyResponse;
	private final HttpHeaders nettyHeaders;


	public NettyHttpResponseHeaders(HttpResponse nettyResponse) {
		this.nettyResponse = nettyResponse;
		this.nettyHeaders = nettyResponse.headers();
	}

	public HttpHeaders delegate(){
		return nettyHeaders;
	}

	@Override
	public boolean contains(String name) {
		return this.nettyHeaders.contains(name);
	}

	@Override
	public boolean contains(String name, String value, boolean ignoreCaseValue) {
		return this.nettyHeaders.contains(name, value, ignoreCaseValue);
	}

	@Override
	public List<Map.Entry<String, String>> entries() {
		return this.nettyHeaders.entries();
	}

	@Override
	public String get(String name) {
		return this.nettyHeaders.get(name);
	}

	@Override
	public List<String> getAll(String name) {
		return this.nettyHeaders.getAll(name);
	}

	@Override
	public Date getDate() throws ParseException {
		return HttpHeaders.getDate(this.nettyResponse);
	}

	@Override
	public Date getDateHeader(String name) throws ParseException {
		return HttpHeaders.getDateHeader(this.nettyResponse, name);
	}

	@Override
	public String getHost() {
		return HttpHeaders.getHost(this.nettyResponse);
	}

	@Override
	public boolean isEmpty() {
		return this.nettyHeaders.isEmpty();
	}

	@Override
	public Set<String> names() {
		return this.nettyHeaders.names();
	}

	@Override
	public long getContentLength() {
		return 0;
	}

	@Override
	public ResponseHeaders add(String name, String value) {
		this.nettyHeaders.add(name, value);
		return this;
	}

	@Override
	public ResponseHeaders add(String name, Iterable<String> values) {
		this.nettyHeaders.add(name, values);
		return this;
	}

	@Override
	public ResponseHeaders addDateHeader(String name, Date value) {
		HttpHeaders.addDateHeader(this.nettyResponse, name, value);
		return this;
	}

	@Override
	public ResponseHeaders clear() {
		this.nettyHeaders.clear();
		return this;
	}

	@Override
	public ResponseHeaders remove(String name) {
		this.nettyHeaders.remove(name);
		return this;
	}

	@Override
	public ResponseHeaders removeTransferEncodingChunked() {
		HttpHeaders.removeTransferEncodingChunked(this.nettyResponse);
		return this;
	}

	@Override
	public ResponseHeaders set(String name, String value) {
		this.nettyHeaders.set(name, value);
		return this;
	}

	@Override
	public ResponseHeaders set(String name, Iterable<String> values) {
		this.nettyHeaders.set(name, values);
		return this;
	}

	@Override
	public ResponseHeaders contentLength(long length) {
		HttpHeaders.setContentLength(this.nettyResponse, length);
		return this;
	}

	@Override
	public ResponseHeaders date(Date value) {
		HttpHeaders.setDate(this.nettyResponse, value);
		return this;
	}

	@Override
	public ResponseHeaders dateHeader(String name, Date value) {
		HttpHeaders.setDateHeader(this.nettyResponse, name, value);
		return this;
	}

	@Override
	public ResponseHeaders dateHeader(String name, Iterable<Date> values) {
		HttpHeaders.setDateHeader(this.nettyResponse, name, values);
		return this;
	}

	@Override
	public ResponseHeaders host(String value) {
		HttpHeaders.setHost(this.nettyResponse, value);
		return this;
	}

	@Override
	public boolean isKeepAlive(){
		return HttpHeaders.isKeepAlive(this.nettyResponse);
	}

	@Override
	public ResponseHeaders keepAlive(boolean keepAlive) {
		HttpHeaders.setKeepAlive(this.nettyResponse, keepAlive);
		return this;
	}

	@Override
	public ResponseHeaders transferEncodingChunked() {
		HttpHeaders.setTransferEncodingChunked(this.nettyResponse);
		return this;
	}
}
