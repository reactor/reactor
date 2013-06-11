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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Utility methods for working with Annotations.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 *
 */
public final class AnnotationUtils {

	private AnnotationUtils() {

	}

	/**
	 * Finds the annotation of the given {@code type} on the given {@code method}.
	 *
	 * @param m The Method to find the annotation an
	 * @param type The type of annotation to find
	 *
	 * @return The annotation that was found, or {@code null}.
	 */
	@SuppressWarnings("unchecked")
	public static <A extends Annotation> A find(Method m, Class<A> type) {
		if (m.getDeclaredAnnotations().length > 0) {
			for (Annotation anno : m.getDeclaredAnnotations()) {
				if (type.isAssignableFrom(anno.getClass())) {
					return (A) anno;
				}
			}
		}
		return null;
	}

}
