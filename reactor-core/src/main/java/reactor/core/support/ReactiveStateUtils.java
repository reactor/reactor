/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.support;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactiveStateUtils {

	/**
	 * Create a "Nodes" and "Links" representation of a given component if available
	 * @param o
	 * @return a Graph
	 */
	public static Graph scan(Object o){
		if(o == null){
			return null;
		}

		Graph graph = new Graph();

		if(ReactiveState.class.isAssignableFrom(o.getClass())){

		}


		return graph;
	}

	/**
	 *
	 */
	public static final class Graph {

		private final List<Node> nodes = new ArrayList<>();
		private final List<Edge> edges = new ArrayList<>();

		public List<Node> getNodes() {
			return nodes;
		}

		public List<Edge> getEdges() {
			return edges;
		}
	}

	/**
	 *
	 */
	public static final class Edge {

	}

	/**
	 *
	 */
	public static final class Node {

	}
}
