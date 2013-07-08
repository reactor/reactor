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

package reactor.core;

/**
 * Base class to encapsulate commonly-used functionality around Reactors.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public abstract class Reactors {

	/**
	 * Create a new {@literal Reactor}.
	 *
	 * @return The new {@link Reactor}.
	 */
	public static ReactorSpec reactor() {
		return new ReactorSpec();
	}

}
