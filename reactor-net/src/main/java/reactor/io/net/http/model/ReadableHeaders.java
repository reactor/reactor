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

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sebastien Deleuze
 */
interface ReadableHeaders {

	boolean contains(String name);

	boolean contains(String name, String value, boolean ignoreCaseValue);

	List<Map.Entry<String, String>> entries();

	String get(String name);

	List<String> getAll(String name);

	Date getDate() throws ParseException;

	Date getDateHeader(String name) throws ParseException;

	String getHost();

	boolean isEmpty();

	Set<String> names();

}
