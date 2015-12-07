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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactiveStateUtils {

	/**
	 * Create an empty graph
	 * @return a Graph
	 */
	public static Graph newGraph() {
		return newGraph(false);
	}

	/**
	 *
	 * @param trace
	 * @return
	 */
	public static Graph newGraph(boolean trace) {
		return new Graph(false, trace);
	}

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @return a Graph
	 */
	public static Graph scan(Object o) {
		return scan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @return a Graph
	 */
	public static Graph scan(Object o, boolean trace) {
		if (o == null) {
			return null;
		}

		Graph graph = new Graph(false, trace);
		Node origin = graph.expandReactiveSate(o, true);
		graph.addUpstream(origin, null);
		graph.addDownstream(origin, null);

		return graph;
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @return a Graph
	 */
	public static Graph subscan(Object o) {
		return subscan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @return a Graph
	 */
	public static Graph subscan(Object o, boolean trace) {
		if (o == null) {
			return null;
		}

		Graph graph = new Graph(true, trace);
		Node root = graph.expandReactiveSate(o);
		graph.addDownstream(root, null);

		return graph;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstream(Object o) {
		return o != null && ReactiveState.Upstream.class.isAssignableFrom(o.getClass()) && ((ReactiveState.Upstream) o).upstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstreams(Object o) {
		return o != null && ReactiveState.LinkedUpstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstream(Object o) {
		return o != null && ReactiveState.Downstream.class.isAssignableFrom(o.getClass()) && ((ReactiveState.Downstream) o).downstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstreams(Object o) {
		return o != null && ReactiveState.LinkedDownstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasFeedbackLoop(Object o) {
		return o != null && ReactiveState.FeedbackLoop.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isTraceOnly(Object o) {
		return o != null && ReactiveState.Trace.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasSubscription(Object o) {
		return o != null && ReactiveState.ActiveUpstream.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isCancellable(Object o) {
		return o != null && ReactiveState.ActiveDownstream.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getCapacity(Object o) {
		if (o != null && ReactiveState.Bounded.class.isAssignableFrom(o.getClass())) {
			return ((ReactiveState.Bounded) o).getCapacity();
		}
		return -1L;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String getName(Object o) {
		if (o == null) {
			return null;
		}

		String name = ReactiveState.Named.class.isAssignableFrom(o.getClass()) ? (((ReactiveState.Named) o).getName()) :
				(o.getClass()
				  .getSimpleName()
				  .isEmpty() ? o.toString() : o.getClass()
				                               .getSimpleName());

		return name.isEmpty() ? "anonymous" : name;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getBuffered(Object o) {
		if (o != null && ReactiveState.Buffering.class.isAssignableFrom(o.getClass())) {
			return ((ReactiveState.Buffering) o).pending();
		}
		return -1L;
	}

	/**
	 *
	 */
	public static final class Graph {

		private final Map<String, Node> nodes = new HashMap<>();
		private final Map<String, Edge> edges = new WeakHashMap<>();

		private final boolean subscan;
		private final boolean trace;

		private boolean cyclic;

		Graph() {
			this(false, false);
		}

		Graph(boolean subscan, boolean trace) {
			this.subscan = subscan;
			this.trace = trace;
		}

		/**
		 *
		 * @param graph
		 * @return
		 */
		public Graph mergeWith(Graph graph) {
			if (graph == null || (graph.nodes.isEmpty() && graph.edges.isEmpty())) {
				return this;
			}
			nodes.putAll(graph.nodes);
			edges.putAll(graph.edges);
			return this;
		}

		/**
		 *
		 * @return
		 */
		public Graph removeTerminatedNodes() {
			Graph removedGraph = new Graph(subscan, trace);
			if (nodes.isEmpty()) {
				return removedGraph;
			}

			for (Node node : nodes.values()) {
				if (node.isTerminated() || node.isCancelled()) {
					nodes.remove(node.getId());
					removedGraph.nodes.put(node.getId(), node);
				}
			}

			return removedGraph;
		}

		/**
		 *
		 * @param o
		 * @return
		 */
		public Node removeNode(Object o) {
			if (o == null) {
				return null;
			}
			return nodes.remove(getName(o) + ":" + o.hashCode());
		}

		public Collection<Node> getNodes() {
			return nodes.values();
		}

		public Collection<Edge> getEdges() {
			return edges.values();
		}

		public boolean isCyclic() {
			return cyclic;
		}

		private void addUpstream(Node target, Node grandchild) {
			if (target == null) {
				return;
			}
			Node child;
			if (trace || !isTraceOnly(target.object)) {
				child = target;
				if (nodes.containsKey(child.getId()) && grandchild != null) {
					cyclic = true;
					return;
				}
				nodes.put(child.getId(), child);
			}
			else {
				child = grandchild;
			}
			if (hasUpstream(target.object)) {
				Node upstream = expandReactiveSate(((ReactiveState.Upstream) target.object).upstream(), child == null);
				if (child != null && (trace || !isTraceOnly(upstream.object))) {
					addEdge(upstream.createEdgeTo(child));
				}
				addUpstream(upstream, child);
			}
			if (hasUpstreams(target.object)) {
				addUpstreams(child, ((ReactiveState.LinkedUpstreams) target.object).upstreams());
			}
			if (hasDownstreams(target.object)) {
				addDownstreams(child, ((ReactiveState.LinkedDownstreams) target.object).downstreams());
			}
		}

		private void addUpstreams(Node target, Iterator o) {
			Node source;
			while (o.hasNext()) {
				source = expandReactiveSate(o.next());
				if (target != null && source != null) {
					addEdge(source.createEdgeTo(target));
				}
				addUpstream(source, target);
			}
		}

		private void addDownstream(Node origin, Node ancestor) {
			if (origin == null) {
				return;
			}
			Node root;
			if (trace || !isTraceOnly(origin.object)) {
				root = origin;
				if (nodes.containsKey(root.getId()) && ancestor != null) {
					cyclic = true;
					return;
				}
				nodes.put(root.getId(), root);
			}
			else {
				root = ancestor;
			}
			if (hasDownstream(origin.object)) {
				Node downstream =
						expandReactiveSate(((ReactiveState.Downstream) origin.object).downstream(), root == null);
				if (root != null && (trace || !isTraceOnly(downstream.object))) {
					addEdge(root.createEdgeTo(downstream));
				}
				addDownstream(downstream, root);
			}
			if (hasDownstreams(origin.object)) {
				addDownstreams(root, ((ReactiveState.LinkedDownstreams) origin.object).downstreams());
			}

			if (hasUpstreams(origin.object)) {
				addUpstreams(root, ((ReactiveState.LinkedUpstreams) origin.object).upstreams());
			}
		}

		private void addDownstreams(Node source, Iterator o) {
			Node downstream;
			while (o.hasNext()) {
				downstream = expandReactiveSate(o.next());
				if (source != null && downstream != null) {
					addEdge(source.createEdgeTo(downstream));
				}
				addDownstream(downstream, source);
			}
		}

		private Node expandReactiveSate(Object o) {
			return expandReactiveSate(o, false);
		}

		private Node expandReactiveSate(Object o, boolean highlight) {
			if (o == null) {
				return null;
			}

			String name = getName(o);
			String id = name.hashCode() + ":" + o.hashCode();

			Node r = new Node(name, id, o, highlight);

			if ((trace || !isTraceOnly(o)) && hasFeedbackLoop(o)) {
				ReactiveState.FeedbackLoop loop = (ReactiveState.FeedbackLoop) o;

				Object target = loop.delegateInput();

				if (target != null && target != loop) {
					Node input = expandReactiveSate(loop.delegateInput());
					addEdge(r.createEdgeTo(input, true));
					addDownstream(input, null);
				}

				target = loop.delegateOutput();

				if (target != null && target != loop) {
					Node output = expandReactiveSate(loop.delegateOutput());
					addEdge(output.createEdgeTo(r, true));
					addUpstream(output, null);
				}
			}

			return r;
		}

		private void addEdge(Edge edge) {
			edges.put(edge.getId(), edge);
		}

		@Override
		public String toString() {
			return "{" +
					" full : " + !subscan +
					", trace : " + trace +
					", edges : " + edges +
					", nodes : " + nodes +
					'}';
		}
	}

	/**
	 *
	 */
	public static class Node implements Comparable<Node> {

		transient private final Object object;
		transient private       Edge[] connectionsRef;
		private final           String id;
		private final           String name;

		private final boolean highlight;

		protected Node(String name, String id, Object o, boolean highlight) {
			this.highlight = highlight;
			this.object = o;
			this.id = id;
			this.name = name;
		}

		private void addEdgeRef(Edge edge){
			if(connectionsRef == null){
				connectionsRef = new Edge[1];
				connectionsRef[0] = edge;
				return;
			}
			int n = connectionsRef.length;
			Edge[] b = new Edge[n + 1];
			System.arraycopy(connectionsRef, 0, b, 0, n);
			b[n] = edge;
			connectionsRef = b;
		}

		public final Object value() {
			return object;
		}

		public final String getId() {
			return id;
		}

		public final String getName() {
			return name;
		}

		public final long getCapacity() {
			return ReactiveStateUtils.getCapacity(object);
		}

		public final long getBuffered() {
			return ReactiveStateUtils.getBuffered(object);
		}

		public final boolean isHighlight() {
			return highlight;
		}

		public final boolean isActive() {
			return !hasSubscription(object) || ((ReactiveState.ActiveUpstream) object).isStarted();
		}

		public final boolean isTerminated() {
			return hasSubscription(object) && ((ReactiveState.ActiveUpstream) object).isTerminated();
		}

		public final boolean isCancelled() {
			return isCancellable(object) && ((ReactiveState.ActiveDownstream) object).isCancelled();
		}

		protected Edge createEdgeTo(Node to) {
			return createEdgeTo(to, false);
		}

		protected Edge createEdgeTo(Node to, boolean discrete) {
			Edge edge = new Edge(id, to.id, discrete);
			addEdgeRef(edge);
			return edge;
		}

		@Override
		public final boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			Node node = (Node) o;

			return id.equals(node.id);
		}

		@Override
		public final int hashCode() {
			return id.hashCode();
		}

		@Override
		public String toString() {
			return "{ id : \"" + id + "\", label : \"" + name + "\" }";
		}

		@Override
		public int compareTo(Node o) {
			return name.compareTo(o.name);
		}
	}

	/**
	 *
	 */
	public static class Edge {

		private final String  from;
		private final String  to;
		private final boolean discrete;

		protected Edge(String from, String to, boolean discrete) {
			this.from = from;
			this.to = to;
			this.discrete = discrete;
		}

		public final String getFrom() {
			return from;
		}

		public final String getTo() {
			return to;
		}

		public final boolean isDiscrete() {
			return discrete;
		}

		public final String getId() {
			return from + "_" + to;
		}

		@Override
		public final boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Edge edge = (Edge) o;

			if (!from.equals(edge.from)) {
				return false;
			}
			return to.equals(edge.to);

		}

		@Override
		public final int hashCode() {
			int result = from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "{ from : \"" + from + "\", to : \"" + to + "\" }";
		}
	}

}
