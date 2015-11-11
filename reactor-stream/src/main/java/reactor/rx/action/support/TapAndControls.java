/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.support;

import reactor.fn.Supplier;
import reactor.core.subscriber.Tap;
import reactor.rx.StreamUtils;
import reactor.rx.action.Control;

/**
 * @author Stephane Maldini
 */
public class TapAndControls<O> implements Control, Supplier<O>{

	private final Control          controls;
	private final Tap<? extends O> tap;

	public TapAndControls(Tap<? extends O> tap, Control controls) {
		this.tap = tap;
		this.controls = controls;
	}

	@Override
	public void requestAll() {
		controls.requestAll();
	}

	@Override
	public void cancel() {
		controls.cancel();
	}

	@Override
	public O get() {
		return tap.get();
	}

	@Override
	public boolean isPublishing() {
		return controls.isPublishing();
	}

	@Override
	public void requestMore(long n) {
		controls.requestMore(n);
	}

	@Override
	public StreamUtils.StreamVisitor debug() {
		return controls.debug();
	}
}
