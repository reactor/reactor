/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.alloc;

/**
 * @author Jon Brisbin
 */
public class RecyclableNumber extends Number implements Recyclable {

	private volatile double value = -1;

	public void setValue(int value) {
		this.value = value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public int intValue() {
		return (int) value;
	}

	@Override
	public long longValue() {
		return (long) value;
	}

	@Override
	public float floatValue() {
		return (float) value;
	}

	@Override
	public double doubleValue() {
		return value;
	}

	@Override
	public void recycle() {
		value = -1;
	}

}
