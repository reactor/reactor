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

import static reactor.core.dynamic.reflect.support.MethodNameUtils.methodNameToSelectorName;

import java.lang.reflect.Method;

import reactor.core.dynamic.annotation.On;
import reactor.core.dynamic.reflect.support.AnnotationUtils;
import reactor.fn.selector.ObjectSelector;
import reactor.fn.selector.Selector;

/**
 * An implementation of {@link MethodSelectorResolver} that looks for an {@link On} annotation
 * or uses the method name minus the "on" portion and lower-casing the first character.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class SimpleMethodSelectorResolver implements MethodSelectorResolver {

	@Override
	public Selector apply(Method method) {
		String sel;
		On onAnno = AnnotationUtils.find(method, On.class);
		if (null != onAnno) {
			sel = onAnno.value();
		} else {
			sel = methodNameToSelectorName(method.getName());
		}

		return (!"".equals(sel) ? new ObjectSelector<String>(sel) : null);
	}

	@Override
	public boolean supports(Method method) {
		return method.getDeclaringClass() != Object.class && !method.getName().contains("$");
	}

}
