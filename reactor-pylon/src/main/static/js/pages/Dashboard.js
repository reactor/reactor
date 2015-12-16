'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Dashboard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Dashboard">
                <section className="dashboard">
                    <div className="section-heading">
                        Dashboard
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Dashboard.propTypes = propTypes;

export default Dashboard;