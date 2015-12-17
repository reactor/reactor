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
import Chartjs       from 'chart.js';
import ReactDOM      from 'react-dom';

const propTypes = {
    controlBus: React.PropTypes.object, chartOptions: React.PropTypes.object
};

class CapacityDoughnut extends React.Component {

    constructor(props) {
        super(props);
        this.disposable = null;

        var thiz = this;
        this.state = {
            dataset: [{
                value: thiz.props.pending, color: "#F7464A", highlight: "#FF5A5E", label: "Pending"
            }, {
                value: thiz.props.max - thiz.props.pending, color: "#46BFBD", highlight: "#5AD3D1", label: "Available"
            }]
        };

        this.doughnut = null;

        if (this.props.controlBus !== undefined) {
            this.props.controlBus.subscribe(this.controlBusHandler.bind(this));
        }
    }

    destroy() {
        if (this.disposable != null) {
            this.disposable.dispose();
        }
        if (this.doughnut != null) {
            this.doughnut.destroy();
        }
    }

    controlBusHandler(event) {
    }

    draw(json) {
        this.state.dataset[0].value = json.pending;
        if (json.max !== undefined) {
            this.state.dataset[1].value = json.max - json.pending;
            // this.state.dataset[2].value = json.max;
        }

        this.setState({dataset: this.state.dataset})
        this.doughnut.update()
    }

    componentWillUnmount() {
        this.destroy();
    }

    componentDidMount() {
        if (this.props.buffers !== undefined) {
            this.disposable = this.props.buffers
                .subscribe(this.draw.bind(this), error => console.log(error), () => console.log("terminated"));
        }
    }

    mount(element){
        if(element !== null) {
            if(this.doughnut !== null){
                this.doughnut.destroy();
            }
            var ctx = element.getContext("2d");
            this.doughnut = new Chartjs(ctx).Doughnut(this.state.dataset, this.props.chartOptions);
        }
    }

    render() {
        var value = (this.state.dataset[1].value / (this.state.dataset[0].value + this.state.dataset[1].value) * 100)
            .toFixed(1);
        return (
            <div className="box" style={{height:'100px', width:'100px'}}>
                <canvas width="200" ref={this.mount.bind(this)}></canvas>
                <span>{value}%</span>
            </div>
        );
    }

}

CapacityDoughnut.propTypes = propTypes;

export default CapacityDoughnut;