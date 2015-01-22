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
package reactor.io.net;

import reactor.rx.Streams;

/**
 * A Streams add-on to work with network facilities from reactor-net, e.g.:
 * <p>
 * <pre>
 * {@code
 * //echo server
 * NetStreams.tcpServer(1234).service( connection ->
 *   connection
 * )
 *
 * NetStreams.tcpClient(1234).connect( output ->
 *   output
 *   .sendAndReceive(Buffer.wrap("hello"))
 *   .onSuccess(log::info)
 * )
 *
 * NetStreams.tcpServer("127.0.0.1", 1234, kryoCodec).service( intput, output -> {
*      input.consume(log::info);
 *     Streams.period(1l).subscribe(output);
 * })
 *
 * NetStreams.tcpClient("localhost", 1234, kryoCodec).connect( output, input -> {
 *   input.consume(log::info);
 *   output.send("hello");
 * })
 *
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public class NetStreams extends Streams {

	private NetStreams() {
	}
}
