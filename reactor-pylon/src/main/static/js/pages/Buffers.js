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
import DocumentTitle from 'react-document-title';
import CapacityDoughnut from '../components/CapacityDoughnut'
import Rx            from 'rx-lite';

const propTypes = {
};

var chartOptions = {
    bezierCurve : true,
    datasetFill : false,
    pointDotStrokeWidth: 2,
    scaleShowVerticalLines: false,
    responsive: true
};

class Buffers extends React.Component {

    constructor(props) {
        super(props);

        this.buffers = this.props.graphStream
            .flatMap(json => Rx.Observable.from(json.streams.nodes))
            .filter(json => json.buffered !== undefined)
            .map(json => {
                return { max: json.capacity, pending: json.buffered }
            });
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Buffers">
                <section className="buffers">
                    <div className="section-heading">
                        Buffers
                    </div>
                    <div className="section-content">
                        <CapacityDoughnut buffers={this.buffers} chartOptions={chartOptions}/>
                    </div>
                </section>
            </DocumentTitle>
        );
    }
}

Buffers.propTypes = propTypes;

export default Buffers;