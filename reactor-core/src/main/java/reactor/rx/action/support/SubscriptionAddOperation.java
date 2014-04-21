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
package reactor.rx.action.support;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import reactor.rx.StreamSubscription;

/**
* @author Stephane Maldini
*/
public class SubscriptionAddOperation<O> extends
		CheckedProcedure<MutableList<StreamSubscription<O>>> {

	static final long serialVersionUID = -7034897190745766939L;

	private final StreamSubscription<O> subscription;

	public SubscriptionAddOperation(StreamSubscription<O> subscription) {
		this.subscription = subscription;
	}

	@Override
	public void safeValue(MutableList<StreamSubscription<O>> object) throws Exception {
		object.add(subscription);
	}
}
