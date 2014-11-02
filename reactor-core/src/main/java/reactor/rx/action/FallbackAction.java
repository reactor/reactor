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

import org.reactivestreams.Publisher;
import reactor.event.dispatch.Dispatcher;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class FallbackAction<T> extends Action<T, T> {

	protected final Publisher<? extends T> fallback;

	private boolean switched        = false;
	private long    pendingRequests = 0l;

	public FallbackAction(Dispatcher dispatcher, Publisher<? extends T> fallback) {
		super(dispatcher);
		this.fallback = fallback;
	}

	@Override
	protected void doNext(T ev) {
		if(switched){
			doFallbackNext(ev);
		}else{
			doNormalNext(ev);
		}
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if((pendingRequests += elements) > 0) pendingRequests = Long.MAX_VALUE;
		super.requestUpstream(capacity, terminated, elements);
	}

	protected void doSwitch(){
		this.cancel();
		this.switched = true;
		fallback.subscribe(this);

		if(pendingRequests > 0){
			upstreamSubscription.request(pendingRequests);
		}
	}

	protected void doNormalNext(T ev) {
		broadcastNext(ev);
	}

	protected void doFallbackNext(T ev) {
		broadcastNext(ev);
	}
}
