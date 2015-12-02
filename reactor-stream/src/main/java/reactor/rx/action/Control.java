/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action;

import reactor.rx.StreamUtils;

/**
 * An interface generally associated with a {@link reactor.rx.Stream} terminal action such as
 * {@link reactor.rx.Stream#consume(reactor.fn.Consumer)}
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public interface Control {

	/**
	 * Stop consuming signals from upstream. Cancel should not be considered blocking.
	 */
	void cancel();

	/**
	 * Check if the current stream is emitting any signal.
	 */
	boolean isTerminated();


	/**
	 * Parse the materialized upstream source to fetch a materialized map form which allows for graph-style printing.
	 *
	 * @return {@link reactor.rx.StreamUtils.StreamVisitor} a Debug container for the current source
	 */
	StreamUtils.StreamVisitor debug();
}
