'use strict';

import React              from 'react';

import Sidebar            from './components/Sidebar';
import Config             from './pages/Config';
import API                from './utils/APIUtils';
import Rx                 from 'rx-lite';

const propTypes = {
  params: React.PropTypes.object,
  nexusStream: React.PropTypes.object,
  logStream: React.PropTypes.object,
  graphStream: React.PropTypes.object,
  systemStream: React.PropTypes.object,
  stateStream: React.PropTypes.object,
  configuration: React.PropTypes.object,
  children: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.object
  ])
};

class App extends React.Component {

  constructor(props) {
      super(props);
      API.defaultOrLastTarget();
      var nexusStream = new Rx.Subject();
      var graphStream = new Rx.ReplaySubject(100);
      var systemStream = new Rx.ReplaySubject(200);
      var logStream = new Rx.ReplaySubject(200);

      nexusStream
          .filter(json => json.type == "GraphEvent")
          .subscribe(graphStream);

      nexusStream
          .filter(json => json.type == "SystemEvent")
          .subscribe(systemStream);

      nexusStream
          .filter(json => json.type == "LogEvent")
          .subscribe(logStream);

      this.state = {
        nexusStream: nexusStream,
        nexusObserver: new Rx.Subject(),
        stateStream: new Rx.BehaviorSubject(API.offline),
        configuration: null,
        graphStream: graphStream,
        logStream: logStream,
        systemStream: systemStream
    };
  }

  componentWillMount() {
      var thiz = this;
      API.ws('stream', this.state.stateStream).then(res => {
          res.receiver.subscribe(thiz.state.nexusStream);
          thiz.state.nexusObserver.subscribe(res.sender);
      }, e => {
          console.log(e);
      });
  }

  componentDidMount() {
  }

  componentWillUnmount() {
  }

  renderChildren() {
    return React.cloneElement(this.props.children, {
      params: this.props.params,
      nexusStream: this.state.nexusStream,
      nexusObserver: this.state.nexusObserver,
      stateStream: this.state.stateStream,
      graphStream: this.state.graphStream,
      logStream: this.state.logStream,
      systemStream: this.state.systemStream,
      configuration: this.state.configuration
    });
  }

  render() {

    if (!this.state.configuration) {
      return (
          <Config {...this.state} />
      );
    }

    return (
      <div>
        <Sidebar {...this.state} />
        <div id='main'>
          {this.renderChildren()}
        </div>
      </div>
    );
  }

}

App.propTypes = propTypes;

export default App;