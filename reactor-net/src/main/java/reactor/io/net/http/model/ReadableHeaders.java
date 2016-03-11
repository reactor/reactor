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

package reactor.io.netty.http.model;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sebastien Deleuze
 */
public interface ReadableHeaders {

	public boolean contains(String name);

	public boolean contains(String name, String value, boolean ignoreCaseValue);

	public List<Map.Entry<String, String>> entries();

	public String get(String name);

	public List<String> getAll(String name);

	public Date getDate() throws ParseException;

	public Date getDateHeader(String name) throws ParseException;

	public String getHost();

	public boolean isEmpty();

	public Set<String> names();

}
