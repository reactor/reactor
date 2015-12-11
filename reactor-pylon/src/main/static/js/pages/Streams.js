'use strict';

import React         from 'react';
import DocumentTitle from 'react-document-title';
import StreamGraph           from '../components/StreamGraph';
import Rx            from 'rx-lite';


class Streams extends React.Component {

    constructor(props) {
        super(props);
        this.resetNodes = new Rx.Subject();
    }

    componentDidUpdate() {
        //this.draw();
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Streams">
                <section className="streams">
                    <div className="section-heading">Stream Monitor <a className="btn btn-primary pull-right" onClick={ e => this.resetNodes.onNext(e)}>Reset</a></div>

                    <StreamGraph fullscreen={true} resetNodes={this.resetNodes} streams={this.props.graphStream.map(json => json.streams)} />
                </section>
            </DocumentTitle>
        );
    }

}

export default Streams;