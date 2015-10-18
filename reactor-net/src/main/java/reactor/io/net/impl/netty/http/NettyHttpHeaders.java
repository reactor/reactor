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

import io.netty.handler.codec.http.HttpRequest;
import reactor.io.net.http.model.HttpHeaders;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyHttpHeaders implements HttpHeaders {

	private final HttpRequest                             nettyRequest;
	private final io.netty.handler.codec.http.HttpHeaders nettyHeaders;

	public NettyHttpHeaders(HttpRequest nettyRequest) {
		this.nettyRequest = nettyRequest;
		this.nettyHeaders = nettyRequest.headers();
	}

	public io.netty.handler.codec.http.HttpHeaders delegate(){
		return nettyHeaders;
	}

	@Override
	public HttpHeaders add(String name, String value) {
		nettyHeaders.add(name, value);
		return this;
	}

	@Override
	public HttpHeaders add(String name, Iterable<String> values) {
		nettyHeaders.add(name, values);
		return this;
	}

	@Override
	public HttpHeaders addDateHeader(String name, Date value) {
		io.netty.handler.codec.http.HttpHeaders.addDateHeader(nettyRequest, name, value);
		return this;
	}

	@Override
	public HttpHeaders clear() {
		nettyHeaders.clear();
		return this;
	}

	@Override
	public HttpHeaders remove(String name) {
		nettyHeaders.remove(name);
		return this;
	}

	@Override
	public HttpHeaders removeTransferEncodingChunked() {
		return this;
	}

	@Override
	public HttpHeaders set(String name, String value) {
		nettyHeaders.set(name, value);
		return this;
	}

	@Override
	public HttpHeaders set(String name, Iterable<String> values) {
		nettyHeaders.set(name, values);
		return this;
	}

	@Override
	public HttpHeaders contentLength(long length) {
		return this;
	}

	@Override
	public HttpHeaders date(Date value) {
		io.netty.handler.codec.http.HttpHeaders.setDate(nettyRequest, value);
		return this;
	}

	@Override
	public HttpHeaders dateHeader(String name, Date value) {
		io.netty.handler.codec.http.HttpHeaders.setDateHeader(nettyRequest, name, value);
		return this;
	}

	@Override
	public HttpHeaders dateHeader(String name, Iterable<Date> values) {
		io.netty.handler.codec.http.HttpHeaders.setDateHeader(nettyRequest, name, values);
		return this;
	}

	@Override
	public HttpHeaders host(String value) {
		io.netty.handler.codec.http.HttpHeaders.setHost(nettyRequest, value);
		return this;
	}

	@Override
	public HttpHeaders keepAlive(boolean keepAlive) {
		io.netty.handler.codec.http.HttpHeaders.setKeepAlive(nettyRequest, keepAlive);
		return this;
	}

	@Override
	public boolean isKeepAlive() {
		return io.netty.handler.codec.http.HttpHeaders.isKeepAlive(nettyRequest);
	}

	@Override
	public HttpHeaders transferEncodingChunked() {
		io.netty.handler.codec.http.HttpHeaders.setTransferEncodingChunked(nettyRequest);
		return this;
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
		return io.netty.handler.codec.http.HttpHeaders.getDate(this.nettyRequest);
	}

	@Override
	public Date getDateHeader(String name) throws ParseException {
		return io.netty.handler.codec.http.HttpHeaders.getDateHeader(this.nettyRequest, name);
	}

	@Override
	public String getHost() {
		return io.netty.handler.codec.http.HttpHeaders.getHost(this.nettyRequest);
	}

	@Override
	public boolean isEmpty() {
		return this.nettyHeaders.isEmpty();
	}

	@Override
	public Set<String> names() {
		return this.nettyHeaders.names();
	}



}
