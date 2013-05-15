/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn;

import java.util.concurrent.TimeUnit;

/**
 * Implementations of this interface can cause the caller to block in waiting for their actions to be performed.
 *
 * @author Jon Brisbin
 */
public interface Deferred<T> {

	/**
	 * Wait until the operation is complete and potentially return a non-null result.
	 *
	 * @return A result if available, {@literal null} otherwise.
	 * @throws InterruptedException
	 */
	T await() throws InterruptedException;

	/**
	 * Wait until the operation is complete for the specified timeout and potentially return a non-null result. If the
	 * timeout condition is reached, that is not considered an error condition and the return value may be null since the
	 * component may not have had time to populate the result.
	 *
	 * @param timeout The timeout value.
	 * @param unit    The unit of time to wait.
	 * @return A result if available, {@literal null} otherwise.
	 * @throws InterruptedException
	 */
	T await(long timeout, TimeUnit unit) throws InterruptedException;

}
