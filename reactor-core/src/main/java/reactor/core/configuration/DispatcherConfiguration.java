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

package reactor.core.configuration;

import reactor.core.dynamic.annotation.Dispatcher;

/**
 * An encapsulation of the configuration for a {@link Dispatcher}.
 *
 * @author Andy Wilkinson
 *
 */
public final class DispatcherConfiguration {

	private final String name;

	private final DispatcherType type;

	private final Integer backlog;

	private final Integer size;

	public DispatcherConfiguration(String name, DispatcherType type, Integer backlog, Integer size) {
		this.name = name;
		this.type = type;
		this.backlog = backlog;
		this.size = size;
	}

	/**
	 * Returns the configured size, or {@code null} if the size was not configured
	 *
	 * @return The size
	 */
	public Integer getSize() {
		return size;
	}

	/**
	 * Returns the configured backlog, or {@code null} if the backlog was not configured
	 *
	 * @return The backlog
	 */
	public Integer getBacklog() {
		return backlog;
	}

	/**
	 * Returns the name if the Dispatcher. Never {@code null}.
	 *
	 * @return The name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the type of the Dispatcher. Never {@code null}.
	 *
	 * @return The type
	 */
	public DispatcherType getType() {
		return type;
	}
}
