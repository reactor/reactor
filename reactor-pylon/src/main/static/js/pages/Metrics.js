'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Metrics extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Metrics">
                <section className="metrics">
                    <div className="section-heading">
                        Map
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Map.propTypes = propTypes;

export default Metrics;