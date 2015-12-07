'use strict';

import React         from 'react';
import Box           from '../components/Box';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';
import vis           from 'vis';
import Rx            from 'rx';

const propTypes = {
    dataset: React.PropTypes.object,
    graph: React.PropTypes.object
};

class System extends React.Component {

    constructor(props) {
        super(props);
    }

    draw() {
        if (this.graph == null) {

            var items = [
                {x: '2014-06-11', y: 10},
                {x: '2014-06-12', y: 25},
                {x: '2014-06-13', y: 30},
                {x: '2014-06-14', y: 10},
                {x: '2014-06-15', y: 15},
                {x: '2014-06-16', y: 30}
            ];
            this.dataset = new vis.DataSet(items);
            var options = {
            };
            var container = document.getElementById('graphProcessor');
            this.graph = new vis.Graph2d(container, this.dataset, options);
        }
    }

    componentDidMount() {
        this.draw();
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • System">
                <section className="system">
                    <div className="section-heading">
                        System
                    </div>
                    <div className="section-content">
                        <Box heading="Processor">
                            <div id="graphProcessor"></div>
                        </Box>
                        <Box heading="Memory">
                            Un autre exemple
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }
}

System.propTypes = propTypes;

export default System;