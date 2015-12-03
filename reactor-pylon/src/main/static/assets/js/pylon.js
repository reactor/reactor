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

var network = null;

var nodes = new vis.DataSet();
var edges = new vis.DataSet();

function destroy() {
    resetAllNodes();
    if (network !== null) {
        network.destroy();
        network = null;
    }
}

var checkTest = false;

function test(){
    checkTest = !checkTest;
    for(var edge in edges.getIds()){
        console.log(edge);
        edges.update({id:edge, arrows:"to", color:checkTest ? "red" : "green"});
    }
}

function resetAllNodes() {
    nodes.clear();
    edges.clear();
}

function resetAllNodesStabilize() {
    resetAllNodes();
    network.stabilize();
}

function draw() {
    destroy();
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
            dragNodes: false
        },
        clickToUse : true,
    };
    network = new vis.Network(container, {}, options);

    //var task =  function(){ test();setTimeout(task, 200); };
    //
    //setTimeout(task, 200);

    // add event listeners
    network.on('select', function (params) {
        document.getElementById('selection').innerHTML = 'Selection: ' + params.nodes;
    });

    // randomly create some nodes and edges
    loadJSON("http://localhost:12012/nexus/stream", function(json){
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

}

$(function(){

    // Main JS
    var navVisible = false;
    var toggle_el = $("[data-toggle]").data('toggle');

    $("[data-toggle]").click(function() {
        $(toggle_el).toggleClass("open-sidebar");
        navVisible = !navVisible;
    });

    $("#content").click(function(){
        if (navVisible) {
            navVisible = false;
            $(toggle_el).removeClass("open-sidebar");
        }
    });
    $(window).resize(function(){
        if ($(this).width() > 800 && navVisible) {
            navVisible = false;
            $(toggle_el).removeClass("open-sidebar");
        }
    });

    var uls = $("#nav").html();
    $("#sidebar").html(uls);


    //FIXME console sample
    draw();

});

function loadJSON(path, success, error) {
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


function getScaleFreeNetwork(nodeCount) {

    var nodes = [];
    var edges = [];
    var connectionCount = [];

    // randomly create some nodes and edges
    for (var i = 0; i < nodeCount; i++) {
        nodes.push({
            id: i,
            label: String(i)
        });

        connectionCount[i] = 0;

        // create edges in a scale-free-network way
        if (i == 1) {
            var from = i;
            var to = 0;
            edges.push({
                from: from,
                to: to
            });
            connectionCount[from]++;
            connectionCount[to]++;
        }
        else if (i > 1) {
            var conn = edges.length * 2;
            var rand = Math.floor(Math.random() * conn);
            var cum = 0;
            var j = 0;
            while (j < connectionCount.length && cum < rand) {
                cum += connectionCount[j];
                j++;
            }


            var from = i;
            var to = j;
            edges.push({
                from: from,
                to: to
            });
            connectionCount[from]++;
            connectionCount[to]++;
        }
    }

    return {nodes:nodes, edges:edges};
}

var randomSeed = 764; // Math.round(Math.random()*1000);
function seededRandom() {
    var x = Math.sin(randomSeed++) * 10000;
    return x - Math.floor(x);
}

var DELAY = 1000; // delay in ms to add new data points

var strategy = document.getElementById('strategy');

// create a graph2d with an (currently empty) dataset
var container = document.getElementById('visualization');
var dataset = new vis.DataSet();

var options = {
    start: vis.moment().add(-30, 'seconds'), // changed so its faster
    end: vis.moment(),
    dataAxis: {
        left: {
            range: {
                min:-10, max: 10
            }
        }
    },
    drawPoints: {
        style: 'circle' // square, circle
    },
    shaded: {
        orientation: 'bottom' // top, bottom
    },
    clickToUse : true
};
var graph2d = new vis.Graph2d(container, dataset, options);

// a function to generate data points
function y(x) {
    return (Math.sin(x / 2) + Math.cos(x / 4)) * 5;
}

function renderStep() {
    // move the window (you can think of different strategies).
    var now = vis.moment();
    var range = graph2d.getWindow();
    var interval = range.end - range.start;
    switch (strategy.value) {
        case 'continuous':
            // continuously move the window
            graph2d.setWindow(now - interval, now, {animation: false});
            requestAnimationFrame(renderStep);
            break;

        case 'discrete':
            graph2d.setWindow(now - interval, now, {animation: false});
            setTimeout(renderStep, DELAY);
            break;

        default: // 'static'
            // move the window 90% to the left when now is larger than the end of the window
            if (now > range.end) {
                graph2d.setWindow(now - 0.1 * interval, now + 0.9 * interval);
            }
            setTimeout(renderStep, DELAY);
            break;
    }
}
renderStep();

/**
 * Add a new datapoint to the graph
 */
function addDataPoint() {
    // add a new data point to the dataset
    var now = vis.moment();
    dataset.add({
        x: now,
        y: y(now / 1000)
    });

    // remove all data points which are no longer visible
    var range = graph2d.getWindow();
    var interval = range.end - range.start;
    var oldIds = dataset.getIds({
        filter: function (item) {
            return item.x < range.start - interval;
        }
    });
    dataset.remove(oldIds);

    setTimeout(addDataPoint, DELAY);
}
addDataPoint();