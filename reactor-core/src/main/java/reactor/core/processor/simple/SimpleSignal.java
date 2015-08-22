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
package reactor.core.processor.simple;

import reactor.core.support.SignalType;

import java.io.Serializable;

/**
 * @author Stephane Maldini
 */
public final class SimpleSignal<T> implements Serializable{

	@SuppressWarnings("unchecked")
	final static SimpleSignal COMPLETE = new SimpleSignal(SignalType.COMPLETE, null, null);
	@SuppressWarnings("unchecked")
	final static SimpleSignal ALERT = new SimpleSignal(null, null, null);

	final public SignalType type;
	final public T          value;
	final public Throwable  error;

	public static <T> SimpleSignal<T> onNext(T value){
		return new SimpleSignal<>(SignalType.NEXT, value, null);
	}

	public static <T> SimpleSignal<T> onError(Throwable error){
		return new SimpleSignal<>(SignalType.ERROR, null, error);
	}

	@SuppressWarnings("unchecked")
	public static <T> SimpleSignal<T> onComplete(){
		return (SimpleSignal<T>) COMPLETE;
	}

	@SuppressWarnings("unchecked")
	public static <T> SimpleSignal<T> alert(){
		return (SimpleSignal<T>) ALERT;
	}

	private SimpleSignal(SignalType type, T value, Throwable error) {
		this.type = type;
		this.value = value;
		this.error = error;
	}
}
