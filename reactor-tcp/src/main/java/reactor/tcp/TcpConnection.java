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

import reactor.core.Stream;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.net.InetSocketAddress;

/**
 * @author Jon Brisbin
 */
public interface TcpConnection<IN, OUT> {

	void close();

	boolean consumable();

	boolean writable();

	InetSocketAddress remoteAddress();

	Stream<IN> in();

	Consumer<OUT> out();

	TcpConnection<IN, OUT> consume(Consumer<IN> consumer);

	Stream<OUT> receive(Function<IN, OUT> fn);

	TcpConnection<IN, OUT> send(Stream<OUT> data);

	TcpConnection<IN, OUT> send(OUT data);

	TcpConnection<IN, OUT> send(OUT data, Consumer<Boolean> onComplete);

}
