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
package reactor.rx.action.conditional;

import reactor.fn.Predicate;
import reactor.rx.action.Action;

/**
 * @author Anatoly Kadyshev
 * @since 2.0
 */
public class ExistsAction<T> extends Action<T, Boolean> {

    private final Predicate<? super T> predicate;

    private boolean elementFound;

    public ExistsAction(Predicate<? super T> predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void doNext(T ev) {
        if (predicate.test(ev)) {
            elementFound = true;
            cancel();
            broadcastNext(true);
            broadcastComplete();
        }
    }

    @Override
    protected void doComplete() {
        if (!elementFound) {
            broadcastNext(false);
        }
        super.doComplete();
    }

}
