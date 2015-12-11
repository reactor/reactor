'use strict';

import React              from 'react';

import Sidebar            from './components/Sidebar';
import Config             from './pages/Config';
import API                from './services/NexusService';
import Rx                 from 'rx-lite';
import ReactDOM           from 'react-dom';

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
        disposable: null,
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

  }

    onConnectError(e){
        this.showConfig();
        console.log(e);
    }

    onConnect(res) {
        if(this.disposable == null) {
            this.disposable = res.receiver.subscribe(this.state.nexusStream);
            this.state.nexusObserver.subscribe(res.sender);
        }
        API.updateTargetAPI()
    }

    showConfig() {
        ReactDOM.render (<Config startCallback={this.start.bind(this)} />, document.getElementById('main'));
    }

    start(apiURL){
        if(this.disposable != null) {
            this.disposable.dispose();
            this.disposable = null;
        }

        API.ws(apiURL, this.state.stateStream).then(this.onConnect.bind(this), this.onConnectError.bind(this));
        //ReactDOM.render (this.renderChildren(), document.getElementById('main'));
    }
  componentDidMount() {
      this.start(API.defaultOrLastTarget())
  }

  componentWillUnmount() {
      if(this.disposable != null){
          this.disposable.dispose();
      }
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

    render(){
        return (
            <div>
                <Sidebar {...this.state} />
                <div id='main'>
                    {this.renderChildren()}
                </div>
            </div>
        )
    }

}

App.propTypes = propTypes;

export default App;