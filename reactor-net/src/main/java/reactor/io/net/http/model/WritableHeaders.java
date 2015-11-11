/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
interface WritableHeaders<T> {

	T add(String name, String value);

	T add(String name, Iterable<String> values);

	T addDateHeader(String name, Date value);

	T clear();

	T remove(String name);

	T removeTransferEncodingChunked();

	T set(String name, String value);

	T set(String name, Iterable<String> values);

	T contentLength(long length);

	T date(Date value);

	T dateHeader(String name, Date value);

	T dateHeader(String name, Iterable<Date> values);

	T host(String value);

	T keepAlive(boolean keepAlive);

	boolean isKeepAlive();

	T transferEncodingChunked();

}
