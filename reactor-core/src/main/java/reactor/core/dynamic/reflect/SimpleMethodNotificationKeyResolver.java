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

package reactor.core.dynamic.reflect;

import static reactor.core.dynamic.reflect.support.MethodNameUtils.methodNameToNotificationKey;

import java.lang.reflect.Method;

import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.reflect.support.AnnotationUtils;

/**
 * An implementation of {@link MethodNotificationKeyResolver} that looks for an {@link Notify} annotation
 * or uses the method name minus the "notify" portion and lower-casing the first character.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public final class SimpleMethodNotificationKeyResolver implements MethodNotificationKeyResolver {

	@Override
	public boolean supports(Method method) {
		return method.getDeclaringClass() != Object.class && !method.getName().contains("$");
	}

	@Override
	public String apply(Method method) {
		String key;
		Notify notifyAnno = AnnotationUtils.find(method, Notify.class);

		if (null != notifyAnno) {
			key = notifyAnno.value();
		} else {
			key = methodNameToNotificationKey(method.getName());
		}

		return key;
	}
}
