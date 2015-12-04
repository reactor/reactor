'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Streams extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="Streams">
                <section className="streams">
                    <div>
                        Streams !
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Streams.propTypes = propTypes;

export default Streams;