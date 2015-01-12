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
package reactor.io;

import reactor.io.stream.ChronicleReaderStream;
import reactor.io.stream.ChronicleStream;
import reactor.rx.Streams;
import reactor.rx.stream.MapStream;

import java.io.IOException;

/**
 * A Streams add-on to work with IO components such as ChronicleStreams.
 * Chronicle backed persistentMap require chronicle 3.3+ in the classpath (not provided transitively by reactor-core)
 *
 * @author Stephane Maldini
 */
public class IOStreams extends Streams {

	private IOStreams() {
	}

	/**
	 * A Persistent Map is a {@link reactor.rx.stream.MapStream} that fulfill both the Map and the Stream contract.
	 *
	 *
	 *
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMap(String name) throws IOException {
		return persistentMap(name, false);
	}

	/**
	 *
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMap(String name, boolean deleteOnExit) throws IOException {
		ChronicleStream<KEY,VALUE> chronicleStream = new ChronicleStream<>(name);
		if(deleteOnExit){
			chronicleStream.deleteOnExit();
		}
		return chronicleStream;
	}

	/**
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <KEY, VALUE> MapStream<KEY, VALUE> persistentMapReader(String name) throws IOException {
		return new ChronicleReaderStream<>(name);
	}


}
