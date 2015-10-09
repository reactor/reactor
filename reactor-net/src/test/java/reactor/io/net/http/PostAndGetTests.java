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
package reactor.io.net.http;

import org.apache.http.HttpException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.io.buffer.Buffer;
import reactor.io.net.NetStreams;
import reactor.io.net.ReactorChannelHandler;
import reactor.rx.Streams;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URLEncoder;
import java.nio.ByteBuffer;

/**
 * @author tjreactive
 * @author smaldini
 */
public class PostAndGetTests {
	private HttpServer<Buffer, Buffer> httpServer;

	@Before
	public void setup() throws InterruptedException {
		setupServer();
	}

	private void setupServer() throws InterruptedException {
		httpServer = NetStreams.httpServer(server -> server.listen(0));
		httpServer.get("/get/{name}", getHandler());
		httpServer.post("/post", postHandler());
		httpServer.start().awaitSuccess();
	}

	ReactorChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> getHandler() {
		return channel -> {
			channel.headers().entries().forEach(entry1 -> System.out.println(String.format("header [%s=>%s]", entry1
			  .getKey
			  (), entry1.getValue())));
			channel.params().entrySet().forEach(entry2 -> System.out.println(String.format("params [%s=>%s]", entry2
			  .getKey
			  (), entry2.getValue())));

			StringBuilder response = new StringBuilder().append("hello ").append(channel.params().get("name"));
			System.out.println(String.format("%s from thread %s", response.toString(), Thread.currentThread()));
			return channel.writeWith(Streams.just(Buffer.wrap(response.toString())));
		};
	}

	ReactorChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> postHandler() {
		return channel -> {

			channel.headers().entries().forEach(entry -> System.out.println(String.format("header [%s=>%s]", entry
				.getKey(),
			  entry.getValue())));

			return channel.writeWith(Streams
			  .wrap(channel)
			  .take(1)
			  .log("received")
			  .flatMap(data -> {
				  final StringBuilder response = new StringBuilder().append("hello ").append(new String(data.asBytes
				    ()));
				  System.out.println(String.format("%s from thread %s", response.toString(), Thread.currentThread()));
				  return Streams.just(Buffer.wrap(response.toString()));
			  }));
		};
	}

	@After
	public void teardown() throws Exception {
		httpServer.shutdown();
	}

	@Test
	public void tryBoth() throws InterruptedException, IOException, HttpException {
		get("/get/joe", httpServer.getListenAddress());
		post("/post", URLEncoder.encode("pete", "UTF8"), httpServer.getListenAddress());
	}

	private void get(String path, SocketAddress address) {
		try {
			StringBuilder request = new StringBuilder().append(String.format("GET %s HTTP/1.1\r\n", path)).append
			  ("Connection: Keep-Alive\r\n").append("\r\n");
			java.nio.channels.SocketChannel channel = java.nio.channels.SocketChannel.open(address);
			System.out.println(String.format("get: request >> [%s]", request.toString()));
			channel.write(Buffer.wrap(request.toString()).byteBuffer());
			ByteBuffer buf = ByteBuffer.allocate(4 * 1024);
			while (channel.read(buf) > -1)
				;
			String response = new String(buf.array());
			System.out.println(String.format("get: << Response: %s", response));
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void post(String path, String data, SocketAddress address) {
		try {
			StringBuilder request = new StringBuilder().append(String.format("POST %s HTTP/1.1\r\n", path)).append
			  ("Connection: Keep-Alive\r\n");
			request.append(String.format("Content-Length: %s\r\n", data.length())).append("\r\n").append(data).append
			  ("\r\n");
			java.nio.channels.SocketChannel channel = java.nio.channels.SocketChannel.open(address);
			System.out.println(String.format("post: request >> [%s]", request.toString()));
			channel.write(Buffer.wrap(request.toString()).byteBuffer());
			ByteBuffer buf = ByteBuffer.allocate(4 * 1024);
			while (channel.read(buf) > -1)
				;
			String response = new String(buf.array());
			System.out.println(String.format("post: << Response: %s", response));
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
