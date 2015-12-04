'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Hosts extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="Hosts">
                <section className="hosts">
                    <div>
                        Hosts !
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Hosts.propTypes = propTypes;

export default Hosts;