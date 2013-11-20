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
package reactor.operations;

import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.util.StringUtils;

import java.util.List;

/**
 * @author Stephane Maldini
 */
public abstract class OperationUtils {

	private static String drawReactorOperations(Reactor reactor, Object successKey, Object failureKey, Object flushKey,
	                                            int d) {
//		List<Registration<? extends Consumer<? extends Event<?>>>> fOperations =
//				reactor.getConsumerRegistry().select(failureKey);

		return loopOperations(reactor.getConsumerRegistry().select(successKey), d, "accept") +
				loopOperations(reactor.getConsumerRegistry().select(flushKey), d, "flush");
	}

	private static String loopOperations(List<Registration<? extends Consumer<? extends Event<?>>>> operations, int d,
	                                     String marker) {
		StringBuilder appender = new StringBuilder();
		for (Registration<?> registration : operations) {
			if (Operation.class.isAssignableFrom(registration.getObject().getClass())) {
				appender.append("\n");
				for (int i = 0; i < d; i++)
					appender.append("|   ");
				appender.append("|____" + marker + ":");

				Operation<?> operation = ((Operation) registration.getObject());
				appender.append(operation.getClass().getSimpleName());

				renderBatch(appender, operation, d);

				appender.append(drawReactorOperations(
						(Reactor) operation.getObservable(),
						operation.getSuccessKey(),
						operation.getFailureKey(),
						null,
						d + 1
				));
			}
		}
		return appender.toString();
	}

	private static void renderBatch(StringBuilder appender, Object consumer, int d) {
		if (BatchOperation.class.isAssignableFrom(consumer.getClass())) {
			BatchOperation operation = (BatchOperation) consumer;
			appender.append(" accepted:" + operation.getAcceptCount());
			appender.append("|errors:" + operation.getErrorCount());
			appender.append("|batchSize:" + operation.getBatchSize());

			appender.append(
					loopOperations(((Reactor) operation.getObservable()).getConsumerRegistry().select(operation.getFlushKey()),
							d + 1, "flush")
			);
			appender.append(
					loopOperations(((Reactor) operation.getObservable()).getConsumerRegistry().select(operation.getFirstKey()),
							d + 1, "first")
			);
			appender.append(
					loopOperations(((Reactor) operation.getObservable()).getConsumerRegistry().select(operation.getLastKey()),
							d + 1, "last")
			);
		}
	}

	public static String browseReactorOperations(Reactor reactor, Object successKey, Object failureKey,
	                                             Object flushKey) {
		StringBuilder appender = new StringBuilder("\nreactor(" + reactor.getId() + ")");
		appender.append(drawReactorOperations(reactor, successKey, failureKey, flushKey, 1));
		return appender.toString();
	}

	public static String browseReactorOperations(Reactor reactor, Object successKey, Object errorKey) {
		return browseReactorOperations(reactor, successKey, errorKey, null);
	}

	public static String browseReactorOperations(Reactor reactor, Object successKey) {
		return browseReactorOperations(reactor, successKey, null, null);
	}

}
