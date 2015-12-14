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

'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import vis           from 'vis';
import ReactDOM      from 'react-dom';

const propTypes = {
    network: React.PropTypes.object, nodes: React.PropTypes.object, edges: React.PropTypes.object
};
var percentColors = [{pct: 0.0, color: {r: 0xff, g: 0x66, b: 0x66}},  {pct: 0.5, color: {r: 0xff, g: 0xff, b: 0x00}},
    {pct: 1.0, color: {r: 0x6d, g: 0xb3, b: 0x3f}}];

const graphUtils = {
    getColorForPercentage(pct) {
        for (var i = 1; i < percentColors.length - 1; i++) {
            if (pct < percentColors[i].pct) {
                break;
            }
        }
        var lower = percentColors[i - 1];
        var upper = percentColors[i];
        var range = upper.pct - lower.pct;
        var rangePct = (pct - lower.pct) / range;
        var pctLower = 1 - rangePct;
        var pctUpper = rangePct;
        var color = {
            r: Math.floor(lower.color.r * pctLower + upper.color.r * pctUpper),
            g: Math.floor(lower.color.g * pctLower + upper.color.g * pctUpper),
            b: Math.floor(lower.color.b * pctLower + upper.color.b * pctUpper)
        };
        return 'rgb(' + [color.r, color.g, color.b].join(',') + ')';
    }
};

class StreamGraph extends React.Component {

    constructor(props) {
        super(props);
        this.disposable = null;

        this.network = null;
        this.nodes = new vis.DataSet();
        this.edges = new vis.DataSet();
        this.graphOptions = this.props.graphOptions;

        if(this.props.controlBus !== undefined){
            this.props.controlBus.subscribe(this.controlBusHandler.bind(this));
        }
    }

    destroy() {
        if (this.disposable != null) {
            this.disposable.dispose();
        }

        this.resetAllNodes();
        if (this.network != null) {
            this.network.destroy();
            this.network = null;
        }
    }

    resetAllNodes() {
        this.nodes.clear();
        this.edges.clear();
    }

    controlBusHandler(event){
        if(event.type  == 'fullscreen'){
            this.fullscreen(event);
        }
        else if(event.type == 'clear'){
            this.resetAllNodes();
        }
        else if(event.type == 'reset'){
            this.resetAllNodesStabilize();
        }
        else if(event.type == 'context'){
            var n = this.nodes.get(event.id);
            if(n != null && n.state != event.state){
                var newcolor = event.state == 'RUNNABLE' ? '#6db33f' : 'indianred';
                this.nodes.update({
                    id: n.id,
                    state: event.state,
                    font: {
                        color: 'white',
                        background: newcolor
                    }
                });
            }
        }
    }

    resetAllNodesStabilize() {
        if (this.network != null) {
            var nodes = this.nodes;
            var edges = this.edges;
            this.network.setData({nodes: nodes, edges: edges});
        }

    }

    fullscreen(e){
        if(this.fullscreenMode === undefined){
            this.fullscreenMode = true;
        }
        else if(this.fullscreenMode){
            this.fullscreenMode = false;
        }
        else{
            this.fullscreenMode = true;
        }

        var container = document.getElementById('stream-graph');
        if(this.fullscreenMode) {
            var h = this.calcHeight();
            container.style.height = (h.y - 110) + "px";
        }
        else{
            container.style.height = "260px";
        }
    }

    draw(json) {
        //this.destroy();
        var container = document.getElementById('stream-graph');

        // height
        if(this.props.fullscreen) {
            var h = this.calcHeight();
            container.style.height = (h.y - 110) + "px";
        }

        var nodes = this.nodes;
        var edges = this.edges;
        if(json.type !== undefined && json.type == 'RemovedGraphEvent'){
            nodes.remove(json.streams);
            //if(this.network != null){
            //    this.network.setData({nodes: nodes, edges: edges});
            //}
            return;
        }
        var network = this.network;
        var first = false;
        if (network == null) {
            var options = Object.assign({
                layout: {
                    randomSeed: 100
                }, interaction: {
                    dragNodes: false, hover: true, tooltipDelay: 0
                }, //clickToUse: true,
                nodes: {
                    labelHighlightBold: false, color: {
                        highlight: {
                            // border: '#6db33f', background: '#34302d'
                        }, border: '#6db33f', background: '#6db33f'
                    }, shape: 'diamond', font: {
                        size: 18, face: 'Montserrat', color: '#34302d'
                    }, borderWidth: 2, scaling: {
                        min: 20, label: {
                            min: 12, max: 18
                        }
                    }
                }, edges: {
                    smooth: true,
                    font: {
                        face: 'Montserrat', color: '#34302d'
                    },
                    //smooth: {
                    //    type: 'dynamic'
                    //},
                    //dashes: true,
                    width: 2
                }, "physics": {
                    //"hierarchicalRepulsion": {
                    //    "nodeDistance": 190
                    //},
                    ////"minVelocity": 0.75,
                    //"barnesHut": {
                    //    "avoidOverlap": 2,
                    //    springLength: 300
                    //},
                    //"solver": "hierarchicalRepulsion"
                }
            }, this.graphOptions);

            network = new vis.Network(container, {}, options);
            this.network = network;
            first = true;
        }

        //var task =  function(){ test();setTimeout(task, 200); };
        //
        //setTimeout(task, 200);

        // randomly create some nodes and edges

        var highlights = [];
        var edgeDetails = {};
        var n, e;
        for (var node in json.nodes) {
            n = json.nodes[node];
            n.label = n.name;

            if (n.highlight) {
                highlights.push(n.id);
            }
            if (n.reference) {
                n.shape = 'icon';
                n.icon = {
                        face: 'FontAwesome',
                        code: '\uf0c2',
                        color: '#00BFFF'
                };

            }
            else if(n.failed !== undefined){
                n.shape = 'icon';
                n.icon = {
                    face: 'FontAwesome',
                    code: '\uf071',
                    color: '#ff6666'
                };
                n.color = {
                    background: 'black'
                }
            }
            else if (n.period !== undefined && n.period > 0) {
                n.shape = 'icon';
                n.icon = {
                    face: 'FontAwesome',
                    code: '\uf017',
                    color: 'purple'
                };

            }
            else if(n.factory !== undefined && n.factory){
                n.shape = 'icon';
                n.icon = {
                    face: 'FontAwesome',
                    code: '\uf013',
                    color: 'black'
                };
            }
            else if (n.active !== undefined && !n.active || n.cancelled || n.terminated) {
                n.shape = "dot";
                n.color = {
                    border: n.terminated ? "#6db33f" : "gray", background: "#f1f1f1"
                };
                if (!n.active && !n.cancelled && !n.terminated) {
                    n.shapeProperties = {borderDashes: [10, 10]};
                }
                n.value = 0;
            }
            else {
                if (n.capacity !== undefined && n.capacity != -1) {
                    n.label = n.label.length > 20 ? n.label.substring(0, 20) + '...' : n.label;
                    n.shape = "dot";
                    if (n.capacity == "unbounded") {
                        n.color = {
                            border: "green", background: "#f1f1f1"
                        };
                        n.shapeProperties = {borderDashes: [10, 10]};
                    }
                    else {
                        n.value = n.capacity;
                        var backgroundColor = n.buffered !== undefined && n.buffered != -1 ?
                            graphUtils.getColorForPercentage(1 - (n.buffered / n.capacity)) : "#6db33f";
                        n.color = {
                            border: "green", background: backgroundColor
                        };
                    }
                }
                else {
                    n.value = 0;
                }
            }

            if(n.requestedDownstream !== undefined && n.requestedDownstream != -1){
                edgeDetails[n.id] = {downstreamRequested: n.requestedDownstream};
            }
            else if(n.expectedUpstream !== undefined && n.expectedUpstream != -1){
                edgeDetails[n.id] = {upstreamRequested: n.expectedUpstream};
            }

            if (n.definedId) {
                n.shape = "square";
            }
            else if(n.inner){
                n.shape = "cicle";
                if(n.color !== undefined && n.color.border !== undefined) {
                    delete n.color.border;
                }
                n.label = n.buffered != -1 ? n.buffered : " ";
            }

        }
        for (var edge in json.edges) {
            e = json.edges[edge];
            var output = null;
            if(edgeDetails[e.from] !== undefined && edgeDetails[e.from].downstreamRequested !== undefined){
                output = edgeDetails[e.from].downstreamRequested;
                //e.value = edgeDetails[e.from].downstreamRequested;
            }
            if(edgeDetails[e.to] !== undefined && edgeDetails[e.to].upstreamRequested !== undefined ){
                output = (output != null ? output +  '/ ' : '') + edgeDetails[e.to].upstreamRequested;
                //e.value = edgeDetails[e.to].upstreamRequested;
            }
            e.arrows = {to: true};
            if(output != null){
                e.label = output;
                e.font = {
                    align: 'top'
                };
            }
            if(e.type !== undefined){
                if(e.type == 'reference') {
                    e.dashes = true;
                    e.arrows = {}
                }
                else if(e.type == 'feedbackLoop') {
                    //e.label = "";
                    e.dashes = true;
                }
                else if(e.type == 'inner') {
                    e.label = "";
                    e.arrows = {}
                }

            }
        }
        nodes.update(json.nodes);
        edges.update(json.edges);

        if (first) {
            // add event listeners
            this.network.on('selectNode', (params) => {
                if(this.props.commands !== undefined){
                    var n = this.nodes.get(params.nodes[0]);
                    if(n.paused === undefined || !n.paused){
                        this.nodes.update({id:n.id, paused: true});
                        this.props.commands.onNext('pause\n'+n.id);
                    }
                    else{
                        this.nodes.update({id:n.id, paused: false});
                        this.props.commands.onNext('resume\n'+n.id);
                    }
                }
                else {
                    document.getElementById('selection').innerHTML = 'Selection: ' + params.nodes[0];
                }
            });
            this.network.on('hoverNode', (params) => {
                ReactDOM.render(<pre className="select">
                {JSON.stringify(nodes.get(params.node), null, 2)}
            </pre>, document.getElementById('selection'));
            });

            network.setData({nodes: nodes, edges: edges});
            network.selectNodes(highlights);
        }


        // cluster inner
        //var clusterOptionsByData = {
        //    joinCondition:function(childOptions) {
        //        return childOptions.inner;
        //    },
        //    processProperties: function(clusterOptions, childNodes) {
        //        clusterOptions.label = "[" + childNodes.length + "]";
        //        return clusterOptions;
        //    },
        //    clusterNodeProperties: {borderWidth:3, shape:'box', font:{size:30}}
        //}
        //network.cluster(clusterOptionsByData);
        //network.clusterByHubsize(undefined, clusterOptionsByData);

        //font: {size:15, color:'red', face:'courier', strokeWidth:3, strokeColor:'#ffffff'}
        // create a network

        return false;
    }

    componentWillUnmount() {
        this.disposable.dispose();
        this.destroy();
    }

    componentDidMount() {
        this.disposable = this.props.streams
            .subscribe(this.draw.bind(this), error => console.log(error), () => console.log("terminated"));
    }

    calcHeight() {
        var w = window, d = document, e = d.documentElement, g = d.getElementsByTagName('body')[0], x = w.innerWidth ||
            e.clientWidth || g.clientWidth, y = w.innerHeight || e.clientHeight || g.clientHeight;

        return {x: x, y: y};
    }

    render() {
        return (
            <div id="stream-graph">
            </div>
        );
    }

}

StreamGraph.propTypes = propTypes;
StreamGraph.graphUtils = graphUtils;


export default StreamGraph;