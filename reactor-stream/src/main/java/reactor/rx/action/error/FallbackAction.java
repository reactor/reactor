/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import reactor.core.support.BackpressureUtils;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FallbackAction<T> extends Action<T, T> {

	protected final Publisher<? extends T> fallback;

	private boolean switched        = false;
	private long    pendingRequests = 0l;

	public FallbackAction(Publisher<? extends T> fallback) {
		this.fallback = fallback;
	}

	@Override
	protected void doNext(T ev) {
		boolean toSwitch;
		synchronized (this){
			if(pendingRequests > 0 && pendingRequests != Long.MAX_VALUE){
				pendingRequests--;
			}
			toSwitch = switched;
		}

		if(toSwitch){
			doFallbackNext(ev);
		}else{
			doNormalNext(ev);
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		synchronized (this) {
			pendingRequests = BackpressureUtils.addOrLongMax(pendingRequests, elements);
		}
		super.requestUpstream(capacity, terminated, elements);
	}

	protected void doSwitch(){
		this.cancel();
		long pending;

		synchronized (this){
			this.switched = true;
			pending = pendingRequests;
		}

		fallback.subscribe(this);

		if(pending > 0){
			upstreamSubscription.request(pending);
		}
	}

	protected void doNormalNext(T ev) {
		broadcastNext(ev);
	}

	protected void doFallbackNext(T ev) {
		broadcastNext(ev);
	}
}
