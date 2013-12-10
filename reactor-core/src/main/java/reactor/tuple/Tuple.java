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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * A {@literal Tuple} is an immutable {@link Collection} of objects, each of which can be of an arbitrary type.
 *
 * @author Jon Brisbin
 */
@SuppressWarnings({"rawtypes"})
public class Tuple implements Iterable {

	protected final Object[] entries;
	protected final int      size;

	/**
	 * Creates a new {@code Tuple} that holds the given {@code values}.
	 *
	 * @param values The values to hold
	 */
	public Tuple(@Nonnull Collection<Object> values) {
		this.entries = (null != values ? values.toArray() : new Object[0]);
		this.size = entries.length;
	}

	/**
	 * Creates a new {@code Tuple} that holds the given {@code values}.
	 *
	 * @param values The values to hold
	 */
	public Tuple(Object... values) {
		this.entries = Arrays.copyOf(values, values.length);
		this.size = values.length;
	}

	/**
	 * Create a {@link Tuple1} with the given object.
	 *
	 * @param t1   The first value in the tuple.
	 * @param <T1> The type of the first value.
	 * @return The new {@link Tuple1}.
	 */
	public static <T1> Tuple1<T1> of(T1 t1) {
		return new Tuple1<T1>(t1);
	}

	/**
	 * Create a {@link Tuple2} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link Tuple2}.
	 */
	public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
		return new Tuple2<T1, T2>(t1, t2);
	}

	/**
	 * Create a {@link Tuple3} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link Tuple3}.
	 */
	public static <T1, T2, T3> Tuple3<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
		return new Tuple3<T1, T2, T3>(t1, t2, t3);
	}

	/**
	 * Create a {@link Tuple4} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @return The new {@link Tuple4}.
	 */
	public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 t1, T2 t2, T3 t3, T4 t4) {
		return new Tuple4<T1, T2, T3, T4>(t1, t2, t3, t4);
	}

	/**
	 * Create a {@link Tuple5} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @return The new {@link Tuple5}.
	 */
	public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
		return new Tuple5<T1, T2, T3, T4, T5>(t1, t2, t3, t4, t5);
	}

	/**
	 * Create a {@link Tuple6} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @return The new {@link Tuple6}.
	 */
	public static <T1, T2, T3, T4, T5, T6> Tuple6<T1, T2, T3, T4, T5, T6> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) {
		return new Tuple6<T1, T2, T3, T4, T5, T6>(t1, t2, t3, t4, t5, t6);
	}

	/**
	 * Create a {@link Tuple7} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param t7   The seventh value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @return The new {@link Tuple7}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Tuple7<T1, T2, T3, T4, T5, T6, T7> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7) {
		return new Tuple7<T1, T2, T3, T4, T5, T6, T7>(t1, t2, t3, t4, t5, t6, t7);
	}

	/**
	 * Create a {@link Tuple8} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param t7   The seventh value in the tuple.
	 * @param t8   The eighth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 * @return The new {@link Tuple8}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8) {
		return new Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>(t1, t2, t3, t4, t5, t6, t7, t8);
	}

	/**
	 * Create a {@link TupleN} with the given objects.
	 *
	 * @param t1      The first value in the tuple.
	 * @param t2      The second value in the tuple.
	 * @param t3      The third value in the tuple.
	 * @param t4      The fourth value in the tuple.
	 * @param t5      The fifth value in the tuple.
	 * @param t6      The sixth value in the tuple.
	 * @param t7      The seventh value in the tuple.
	 * @param t8      The eighth value in the tuple.
	 * @param tRest   The rest of the values.
	 * @param <T1>    The type of the first value.
	 * @param <T2>    The type of the second value.
	 * @param <T3>    The type of the third value.
	 * @param <T4>    The type of the fourth value.
	 * @param <T5>    The type of the fifth value.
	 * @param <T6>    The type of the sixth value.
	 * @param <T7>    The type of the seventh value.
	 * @param <TRest> The type of the last tuple.
	 * @return The new {@link Tuple8}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, TRest extends Tuple> TupleN<T1, T2, T3, T4, T5, T6, T7, T8, TRest> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8, Object... tRest) {
		return new TupleN<T1, T2, T3, T4, T5, T6, T7, T8, TRest>(t1, t2, t3, t4, t5, t6, t7, t8, new Tuple(tRest));
	}

	/**
	 * Get the object at the given index.
	 *
	 * @param index The index of the object to retrieve. Starts at 0.
	 * @return The object. Might be {@literal null}.
	 */
	@Nullable
	public Object get(int index) {
		return (size > 0 && size > index ? entries[index] : null);
	}

	/**
	 * Turn this {@literal Tuple} into a plain Object array.
	 *
	 * @return A new Object array.
	 */
	public Object[] toArray() {
		return entries;
	}

	/**
	 * Return the number of elements in this {@literal Tuple}.
	 *
	 * @return The size of this {@literal Tuple}.
	 */
	public int size() {
		return size;
	}

	@Override
	@Nonnull
	public Iterator<?> iterator() {
		return Arrays.asList(entries).iterator();
	}


  @Override
  public int hashCode() {
    if (this.size == 0) {
      return 0;
    } else if (this.size == 1) {
      return this.entries[0].hashCode();
    } else {
      int hashCode = 1;
      for (Object entry: this.entries) {
        hashCode = hashCode ^ entry.hashCode();
      }
      return hashCode;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;

    if (!(o instanceof Tuple)) return false;

    Tuple cast = (Tuple)o;

    if (this.size != cast.size) return false;

    boolean eq = true;

    for (int i = 0; i < this.size; i++) {
      if (!this.entries[i].equals(cast.entries[i])) {
        return false;
      }
    }

    return eq;
  }
}
