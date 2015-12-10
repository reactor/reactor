'use strict';

import React                       from 'react';
import {Router, Route, IndexRoute} from 'react-router';
import CreateBrowserHistory        from 'history/lib/createBrowserHistory';

import App                         from './App';
import Dashboard                   from './pages/Dashboard';
import Metrics                     from './pages/Metrics';
import Streams                     from './pages/Streams';
import Hosts                       from './pages/Hosts';
import System                      from './pages/System';
import Studio                      from './pages/Studio';
import NotFoundPage                from './pages/NotFoundPage';

export default (
  <Router history={CreateBrowserHistory()}>

    <Route prefix="/pylon" component={App}>
      <IndexRoute component={Dashboard}/>
      <Route path="/pylon/dashboard" component={Dashboard}  />
      <Route path="/pylon/metrics" component={Metrics} />
      <Route path="/pylon/streams" component={Streams}/>
      <Route path="/pylon/hosts" component={Hosts} />
      <Route path="/pylon/system" component={System} />
      <Route path="/pylon/studio" component={Studio} />
      <Route path="/pylon" component={Dashboard} />
      <Route path="*" component={NotFoundPage} />
    </Route>

  </Router>
);
