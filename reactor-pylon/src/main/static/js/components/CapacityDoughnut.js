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
import {Doughnut }      from 'react-chartjs';
import ReactDOM      from 'react-dom';

const propTypes = {
    controlBus: React.PropTypes.object,
    chartOptions: React.PropTypes.object
};

class CapacityDoughnut extends React.Component {

    constructor(props) {
        super(props);
        this.disposable = null;

        this.state = {
            dataset: [
                {
                    value: 0,
                    color:"#F7464A",
                    highlight: "#FF5A5E",
                    label: "Pending"
                },
                {
                    value: 0,
                    color: "#46BFBD",
                    highlight: "#5AD3D1",
                    label: "Available"
                },
                {
                    value: 0,
                    color: "#FDB45C",
                    highlight: "#FFC870",
                    label: "Max Capacity"
                }
            ]
        };

        if(this.props.controlBus !== undefined){
            this.props.controlBus.subscribe(this.controlBusHandler.bind(this));
        }
    }

    destroy() {
        if (this.disposable != null) {
            this.disposable.dispose();
        }
    }

    controlBusHandler(event){
    }

    init(doughnut){
        this.doughnut = doughnut;
    }

    draw(json) {
        this.state.dataset[0].value = json.pending;
        if(json.max !== undefined) {
            this.state.dataset[1].value = json.max - json.pending;
            this.state.dataset[2].value = json.max;
        }

        if(this.doughnut == null){
            return;
        }
        this.doughnut.update();
    }

    componentWillUnmount() {
        this.destroy();
    }

    componentDidMount() {
        this.disposable = this.props.buffers
            .subscribe(this.draw.bind(this), error => console.log(error), () => console.log("terminated"));
    }

    render() {
        return (
            <Doughnut ref={this.init.bind(this)}
                      data={this.state.dataset}
                      options={this.props.chartOptions} height="200" />
        );
    }

}

CapacityDoughnut.propTypes = propTypes;


export default CapacityDoughnut;