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

package reactor.core.dynamic.reflect.support;

/**
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public abstract class MethodNameUtils {

	protected MethodNameUtils() {
	}

	/**
	 * Strip the "on" or "notify" when a method name and split the camel-case into a dot-separated {@literal String}.
	 *
	 * @param name The method name to transform.
	 * @return A camel-case-to-dot-separated version suitable for a {@link reactor.fn.selector.Selector}.
	 */
	public static String methodNameToSelectorName(String name) {
		return doTransformation(name, "on", "^on\\.");
	}

	/**
	 * Strip the "notify" when a method name and split the camel-case into a dot-separated {@literal String}.
	 *
	 * @param name The method name to transform.
	 * @return A camel-case-to-dot-separated version suitable for a notification key
	 */
	public static String methodNameToNotificationKey(String name) {
		return doTransformation(name, "notify", "^notify\\.");
	}

	private static String doTransformation(String name, String prefix, String replacementRegex) {
		String s = name.replaceAll("([A-Z])", ".$1").toLowerCase();
		if (s.startsWith(prefix)) {
			return s.replaceFirst(replacementRegex, "");
		} else {
			return null;
		}
	}

}
