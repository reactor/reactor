'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';
import vis           from 'vis';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Streams extends React.Component {

    constructor(props) {
        super(props);
        this.network = null;

        this.nodes = new vis.DataSet();
        this.edges = new vis.DataSet();
    }

    destroy() {
        this.resetAllNodes();
        if (this.network !== null) {
            this.network.destroy();
            this.network = null;
        }
    }

    resetAllNodes() {
        this.nodes.clear();
        this.edges.clear();
    }

    resetAllNodesStabilize() {
        this.resetAllNodes();
        this.network.stabilize();
    }

    loadJSON(path, success, error) {
        var xhr = new XMLHttpRequest();
        xhr.onreadystatechange = function () {
            if (xhr.readyState === 4) {
                if (xhr.status === 200) {
                    success(JSON.parse(xhr.responseText));
                }
                else {
                    error(xhr);
                }
            }
        };
        xhr.open('GET', path, true);
        xhr.send();
    }

    draw() {
        this.destroy();
        var container = document.getElementById('mynetwork');
        var options = {
            layout: {
                hierarchical: {
                    direction: "LR",
                    sortMethod: "directed"
                },
                randomSeed:2
            },
            interaction: {
                dragNodes: false,
                zoomView: false
            },
            clickToUse : true,
            nodes: {
                color: {
                    highlight:{
                        border:'#6db33f',
                        background:'#34302d'
                    },
                    border:'#6db33f',
                    background:'#6db33f'
                },
                shape: 'dot',
                font: {
                    size: 18,
                    face: 'Montserrat',
                    color: '#34302d',
                },
                borderWidth: 2
            },
            edges: {
                dashes: true,
                width: 2
            }
        };
        this.network = new vis.Network(container, {}, options);

        //var task =  function(){ test();setTimeout(task, 200); };
        //
        //setTimeout(task, 200);

        // add event listeners
        this.network.on('select', function (params) {
            document.getElementById('selection').innerHTML = 'Selection: ' + params.nodes;
        });

        var nodes = this.nodes;
        var edges = this.edges;
        var network = this.network;
        // randomly create some nodes and edges
        this.loadJSON("http://localhost:12012/nexus/stream", function(json){
            //var data = getScaleFreeNetwork(nodeCount)
            nodes.add(json.nodes);
            var highlights = [];
            for(var node in json.nodes){
                if(json.nodes[node].highlight){
                    highlights.push(json.nodes[node].id);
                }
            }
            for(var edge in json.edges){
                json.edges[edge].arrows = {to:true};
            }
            edges.add(json.edges);
            network.setData({nodes: nodes, edges: edges});
            network.selectNodes(highlights);

            //font: {size:15, color:'red', face:'courier', strokeWidth:3, strokeColor:'#ffffff'}
            // create a network

        }, function(error){
            console.log(error);
        });
        return false;
    }

    componentDidMount(){
        this.draw();
    }

    componentDidUpdate(){
        this.draw();
    }

    render() {
        return (
            <DocumentTitle title="Streams">
                <section className="streams">
                    <h2>Stream Monitor <a href="#" onClick={this.draw}>Reset</a></h2>

                    <div id="mynetwork"></div>

                    <p id="selection"></p>
                </section>
            </DocumentTitle>
        );
    }

}

Streams.propTypes = propTypes;

export default Streams;