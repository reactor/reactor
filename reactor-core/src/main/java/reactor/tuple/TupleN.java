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

package reactor.tuple;

import reactor.util.ObjectUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A tuple that holds 9 or more values
 *
 * @param <T1>    The type of the first value held by this tuple
 * @param <T2>    The type of the second value held by this tuple
 * @param <T3>    The type of the third value held by this tuple
 * @param <T4>    The type of the fourth value held by this tuple
 * @param <T5>    The type of the fifth value held by this tuple
 * @param <T6>    The type of the sixth value held by this tuple
 * @param <T7>    The type of the seventh value held by this tuple
 * @param <T8>    The type of the eighth value held by this tuple
 * @param <TRest> The type of the tuple that holds the remaining values
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class TupleN<T1, T2, T3, T4, T5, T6, T7, T8, TRest extends Tuple> extends Tuple8<T1, T2, T3, T4, T5, T6, T7,
		T8> {

	private static final long serialVersionUID = 666954435584703227L;

	public final Object[] entries;

	@SuppressWarnings("unchecked")
	TupleN(Object... values) {
		super(9, null,null,null,null,null,null,null,null);
		this.entries = Arrays.copyOf(values, values.length);;
	}

	/**
	 * Type-safe way to get the remaining objects of this {@link Tuple}.
	 *
	 * @return The remaining objects, as a Tuple.
	 */
	@SuppressWarnings("unchecked")
	public TRest getTRest() {
		return (TRest) entries[8];
	}

	@Nullable
	@Override
	public Object get(int index) {
		return (size > 0 && size > index ? entries[index] : null);
	}

	@Override
	public Object[] toArray() {
		return entries;
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(entries).iterator();
	}

	@Override
	public int hashCode() {
		if (this.size == 0) {
			return 0;
		} else if (this.size == 1) {
			return ObjectUtils.nullSafeHashCode(this.entries[0]);
		} else {
			int hashCode = 1;
			for (Object entry: this.entries) {
				hashCode = hashCode ^ ObjectUtils.nullSafeHashCode(entry);
			}
			return hashCode;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) return false;

		if (!(o instanceof TupleN)) return false;

		TupleN cast = (TupleN)o;

		if (this.size != cast.size) return false;

		for (int i = 0; i < this.size; i++) {
			if (null != this.entries[i] && !this.entries[i].equals(cast.entries[i])) {
				return false;
			}
		}

		return true;
	}
}
