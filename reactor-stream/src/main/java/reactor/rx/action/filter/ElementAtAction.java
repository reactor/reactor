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
package reactor.rx.action.filter;

import reactor.rx.action.Action;

/**
 * @author Anatoly Kadyshev
 *
 * @since 2.0
 */
public class ElementAtAction<T> extends Action<T, T> {

    private final int index;

    private final T defaultValue;

    private final boolean defaultProvided;

    private int currentIndex = 0;

    public ElementAtAction(int index) {
        this(index, null, false);
    }

    public ElementAtAction(int index, T defaultValue) {
        this(index, defaultValue, true);
    }

    public ElementAtAction(int index, T defaultValue, boolean defaultProvided) {
        this.defaultValue = defaultValue;
        this.defaultProvided = defaultProvided;
        if (index < 0) {
            throw new IndexOutOfBoundsException("index should be >= 0");
        }
        this.index = index;
    }

    @Override
    protected void doNext(T ev) {
        if (currentIndex == index) {
            cancel();
            broadcastNext(ev);
            broadcastComplete();
        }
        currentIndex++;
    }

    @Override
    public void doComplete() {
        if (currentIndex <= index) {
            if (defaultProvided) {
                broadcastNext(defaultValue);
            } else {
                broadcastError(new IndexOutOfBoundsException("index is out of bounds"));
                return;
            }
        }
        super.doComplete();
    }
}
