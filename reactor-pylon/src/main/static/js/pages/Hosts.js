'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';
import Box           from '../components/Box';

const propTypes = {
};

var chartLineOptions = {
    bezierCurve : true,
    datasetFill : false,
    pointDotStrokeWidth: 2,
    scaleShowVerticalLines: false,
    responsive: true
};

class Hosts extends React.Component {

    constructor(props) {
        super(props);

        this.state = {
            chartLineData: {
                labels: ["January", "February", "March", "April", "May", "June", "July"],
                datasets: [
                    {
                        fillColor: "#25BDFF",
                        strokeColor: "#25BDFF",
                        pointColor: "#25BDFF",
                        pointStrokeColor: "#25BDFF",
                        pointHighlightFill: "#25BDFF",
                        pointHighlightStroke: "#25BDFF",
                        data: [28, 48, 40, 19, 86, 27, 90]
                    }
                ]
            },
            chartDonutData: [
                {
                    value: 300,
                    color:"#F7464A",
                    highlight: "#FF5A5E",
                    label: "Red"
                },
                {
                    value: 50,
                    color: "#46BFBD",
                    highlight: "#5AD3D1",
                    label: "Green"
                },
                {
                    value: 100,
                    color: "#FDB45C",
                    highlight: "#FFC870",
                    label: "Yellow"
                }
            ]
        };
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Hosts">
                <section className="hosts">
                    <div className="section-heading">
                        Sample chart.js
                    </div>
                    <div className="section-content">
                        <Box heading="Line Chart">
                            <div className="box-padding">
                                <div className="box-wrap">
                                    {/*<Line data={this.state.chartLineData} options={chartLineOptions} height="200" />*/}
                                </div>
                            </div>
                        </Box>
                        <Box heading="Donut Chart">
                            <div className="box-padding">
                                <div className="box-wrap">
                                   {/* <Doughnut data={this.state.chartDonutData} options={chartLineOptions} height="200" />*/}
                                </div>
                            </div>
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Hosts.propTypes = propTypes;

export default Hosts;