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
    responsive: true,
    animateRotate : false,
    animateScale : false
};

class Buffers extends React.Component {

    constructor(props) {
        super(props);

        this.buffers = {};
        this.disposable = null;
    }

    componentWillUnmount(){
        this.buffers = null;
        if(this.disposable !== null) {
            this.disposable.dispose();
        }
    }

    componentDidMount(){
        var thiz = this;
        this.disposable = this.props.graphStream
            .flatMap(json => Rx.Observable.from(json.streams.nodes))
            .filter(json => json.buffered !== undefined)
            .subscribe(json => {
                if(thiz.buffers[json.id] === undefined) {
                    var s = new Rx.Subject();
                    thiz.buffers[json.id] = ({id: json.id, max: json.capacity, pending: json.buffered, stream: s});
                    thiz.setState({buffers: thiz.buffers})
                }
                else{
                    thiz.buffers[json.id].max = json.capacity;
                    thiz.buffers[json.id].pending = json.buffered;
                    thiz.buffers[json.id].stream.onNext(thiz.buffers[json.id]);
                }

            });
    }

    render() {
        var list = [];
        for( var b in this.buffers){
            list.push(<CapacityDoughnut key={b}
                                        chartOptions={chartOptions}
                                        max={this.buffers[b].max}
                                        buffers={this.buffers[b].stream}
                                        pending={this.buffers[b].pending} />)
        }
        return (
            <DocumentTitle title="Reactor Console • Buffers">
                <section className="buffers">
                    <div className="section-heading">
                        Buffers
                    </div>
                    <div className="section-content">
                        {list}
                    </div>
                </section>
            </DocumentTitle>
        );
    }
}

//<CapacityDoughnut max={res.max} pending={res.pending} buffers={res.stream} chartOptions={chartOptions}/>
Buffers.propTypes = propTypes;

export default Buffers;