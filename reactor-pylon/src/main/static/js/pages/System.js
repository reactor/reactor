'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class System extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="System">
                <section className="system">
                    <div>
                        System !
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

System.propTypes = propTypes;

export default System;