/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.tcp;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.encoding.json.JsonCodec;
import reactor.tcp.netty.NettyTcpServer;

/**
 * @author Jon Brisbin
 */
@Ignore
public class TcpServerTests {

	Environment env;

	@Before
	public void loadEnv() {
		env = new Environment();
	}

	@Test
	public void testTcpServer() throws InterruptedException {
		TcpServer<Pojo, Pojo> server = new TcpServer.Spec<Pojo, Pojo>(NettyTcpServer.class)
				.using(env)
				.dispatcher(Environment.EVENT_LOOP)
				.codec(new JsonCodec<Pojo, Pojo>(Pojo.class, Pojo.class))
				.consume(new Consumer<TcpConnection<Pojo, Pojo>>() {
					@Override
					public void accept(TcpConnection<Pojo, Pojo> conn) {
						conn.consume(new Consumer<Pojo>() {
							@Override
							public void accept(Pojo data) {
								System.out.println("got data: " + data);
							}
						});
					}
				})
				.get()
				.start();

		while (true) {
			Thread.sleep(5000);
		}
	}

	private static class Pojo {
		String name;

		private Pojo() {
		}

		private Pojo(String name) {
			this.name = name;
		}

		private String getName() {
			return name;
		}

		private void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Pojo{" +
					"name='" + name + '\'' +
					'}';
		}
	}

}
