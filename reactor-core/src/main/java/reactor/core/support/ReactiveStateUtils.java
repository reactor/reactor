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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactiveStateUtils {


	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @param o
	 * @return a Graph
	 */
	public static Graph scan(Object o){
		return scan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @param o
	 * @param trace
	 * @return a Graph
	 */
	public static Graph scan(Object o, boolean trace){
		if(o == null){
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
	 * @param o
	 * @return a Graph
	 */
	public static Graph subscan(Object o){
		return subscan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @param o
	 * @param trace
	 * @return a Graph
	 */
	public static Graph subscan(Object o, boolean trace){
		if(o == null){
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
	public static boolean hasUpstreams(Object o){
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
	public static boolean hasDownstreams(Object o){
		return o != null && ReactiveState.LinkedDownstreams.class.isAssignableFrom(o.getClass());
	}


	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasFeedbackLoop(Object o){
		return o != null && ReactiveState.FeedbackLoop.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isTraceOnly(Object o){
		return o != null && ReactiveState.Trace.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 */
	public static final class Graph {

		private final Set<Node> nodes = new HashSet<>();
		private final Set<Edge> edges = new HashSet<>();

		private final boolean subscan;
		private final boolean trace;

		Graph(){
			this(false, false);
		}

		Graph(boolean subscan, boolean trace){
			this.subscan = subscan;
			this.trace = trace;
		}

		public Set<Node> getNodes() {
			return nodes;
		}

		public Set<Edge> getEdges() {
			return edges;
		}

		private void addUpstream(Node target, Node grandchild){
			Node child;
			if(trace || !isTraceOnly(target.object)) {
				child = target;
				if(!nodes.add(child)){
					return;
				}
			}
			else{
				child = grandchild;
			}
			if(hasUpstream(target.object)){
				Node upstream =  expandReactiveSate(((ReactiveState.Upstream)target.object).upstream(), child == null);
				if(child != null && (trace || !isTraceOnly(upstream.object))) {
					edges.add(upstream.createEdgeTo(child));
				}
				addUpstream(upstream, child);
			}
			if(hasUpstreams(target.object)){
				addUpstreams(child, ((ReactiveState.LinkedUpstreams)target.object).upstreams());
			}
			if(hasDownstreams(target.object)){
				addDownstreams(child, ((ReactiveState.LinkedDownstreams)target.object).downstreams());
			}
		}

		private void addUpstreams(Node target, Iterator o){
			Node source;
			while(o.hasNext()){
				source = expandReactiveSate(o.next());
				if(target != null && source != null) {
					edges.add(source.createEdgeTo(target));
				}
				addUpstream(source, target);
			}
		}

		private void addDownstream(Node origin, Node ancestor){
			Node root;
			if(trace || !isTraceOnly(origin.object)) {
				root = origin;
				if(!nodes.add(root)){
					return;
				}
			}
			else{
				root = ancestor;
			}
			if(hasDownstream(origin.object)){
				Node downstream = expandReactiveSate(((ReactiveState.Downstream)origin.object).downstream(), root == null);
				if(root != null && (trace || !isTraceOnly(downstream.object))) {
					edges.add(root.createEdgeTo(downstream));
				}
				addDownstream(downstream, root);
			}
			if(hasDownstreams(origin.object)){
				addDownstreams(root, ((ReactiveState.LinkedDownstreams)origin.object).downstreams());
			}

			if(hasUpstreams(origin.object)){
				addUpstreams(root, ((ReactiveState.LinkedUpstreams)origin.object).upstreams());
			}
		}

		private void addDownstreams(Node source, Iterator o){
			Node downstream;
			while(o.hasNext()){
				downstream = expandReactiveSate(o.next());
				if(source != null && downstream != null) {
					edges.add(source.createEdgeTo(downstream));
				}
				addDownstream(downstream, source);
			}
		}

		private Node expandReactiveSate(Object o){
			return expandReactiveSate(o, false);
		}

		private Node expandReactiveSate(Object o, boolean highlight){
			String name = ReactiveState.Named.class.isAssignableFrom(o.getClass()) ?
					(((ReactiveState.Named)o).getName()) :
					(o.getClass().getSimpleName().isEmpty() ? o.toString() : o.getClass().getSimpleName());
			name = name.isEmpty() ? "anonymous" : name;

			String id = name+":"+o.hashCode();

			Node r = new Node(name, id, o, highlight);

			if(trace && hasFeedbackLoop(o)){
				ReactiveState.FeedbackLoop loop = (ReactiveState.FeedbackLoop)o;
				Node input = expandReactiveSate(loop.delegateInput());
				edges.add(r.createEdgeTo(input));
				addDownstream(input, null);
				Node output = expandReactiveSate(loop.delegateOutput());
				addUpstream(output, null);
				edges.add(output.createEdgeTo(r));
			}

			return r;
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
	public static class Node {
		transient private final Object object;
		private final String id;
		private final String name;
		private final boolean highlight;

		protected Node(String name, String id, Object o, boolean highlight){
			this.highlight = highlight;
			this.object = o;
			this.id = id;
			this.name = name;
		}

		public final Object value() {
			return object;
		}

		public final String getId() {
			return id;
		}

		public final String getLabel() {
			return name;
		}

		public final boolean isHighlight(){
			return highlight;
		}


		protected Edge createEdgeTo(Node to){
			return new Edge(id, to.id);
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
			return "{ id : \"" + id +"\", label : \"" + name + "\" }";
		}
	}

	/**
	 *
	 */
	public static class Edge {
		private final String from;

		private final String to;

		protected Edge(String from, String to){
			this.from = from;
			this.to = to;
		}

		public final String getFrom() {
			return from;
		}

		public final String getTo() {
			return to;
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
		public  String toString() {
			return "{ from : \"" + from + "\", to : \""+to+"\" }";
		}
	}

}
