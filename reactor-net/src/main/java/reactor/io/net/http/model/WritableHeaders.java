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

package reactor.io.net.http.model;

import java.util.Date;

/**
 * @author Sebastien Deleuze
 */
public interface WritableHeaders<T> {

	public T add(String name, String value);

	public T add(String name, Iterable<String> values);

	public T addDateHeader(String name, Date value);

	public T clear();

	public T remove(String name);

	public T removeTransferEncodingChunked();

	public T set(String name, String value);

	public T set(String name, Iterable<String> values);

	public T contentLength(long length);

	public T date(Date value);

	public T dateHeader(String name, Date value);

	public T dateHeader(String name, Iterable<Date> values);

	public T host(String value);

	public T keepAlive(boolean keepAlive);

	public T transferEncodingChunked();

}
