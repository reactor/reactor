'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
    currentUser: React.PropTypes.object
};

class Map extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <DocumentTitle title="Map">
                <section className="map">
                    <div>
                        Map !
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Map.propTypes = propTypes;

export default Map;