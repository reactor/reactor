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
package reactor.rx.action;

import org.reactivestreams.spi.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Supplier;

/**
 * @author Stephane Maldini
 */
public class SupplierAction<T,V> extends Action<T, V> {

	private final Supplier<V> supplier;

	public SupplierAction(Dispatcher dispatcher, Supplier<V> supplier) {
		super(dispatcher);
		this.supplier = supplier;
	}

	@Override
	protected void drain(int elements, Subscriber<V> subscriber) {
		if(getSubscription() == null){
			subscriber.onNext(supplier.get());
		}else{
			super.drain(elements,subscriber);
		}
	}

	@Override
	protected void doFlush() {
		broadcastNext(supplier.get());
	}
}
