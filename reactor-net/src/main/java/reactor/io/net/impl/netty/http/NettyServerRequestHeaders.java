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
import io.netty.handler.codec.http.HttpRequest;
import reactor.io.net.http.model.RequestHeaders;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyServerRequestHeaders implements RequestHeaders {

	private final HttpRequest nettyRequest;
	private final HttpHeaders nettyHeaders;

	public NettyServerRequestHeaders(HttpRequest nettyRequest) {
		this.nettyRequest = nettyRequest;
		this.nettyHeaders = nettyRequest.headers();
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
		return HttpHeaders.getDate(this.nettyRequest);
	}

	@Override
	public Date getDateHeader(String name) throws ParseException {
		return HttpHeaders.getDateHeader(this.nettyRequest, name);
	}

	@Override
	public String getHost() {
		return HttpHeaders.getHost(this.nettyRequest);
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
