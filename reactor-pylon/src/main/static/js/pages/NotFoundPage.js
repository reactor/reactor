'use strict';

import React         from 'react';
import DocumentTitle from 'react-document-title';

const propTypes = {
  currentUser: React.PropTypes.object
};

class NotFoundPage extends React.Component {

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <DocumentTitle title="404: Not Found">
        <section className="not-found-page">

          Page Not Found

        </section>
      </DocumentTitle>
    );
  }

}

NotFoundPage.propTypes = propTypes;

export default NotFoundPage;