'use strict';

import React                       from 'react';
import {Router, Route, IndexRoute} from 'react-router';
import CreateBrowserHistory        from 'history/lib/createBrowserHistory';

import App                         from './App';
import Dashboard                   from './pages/Dashboard';
import Map                         from './pages/Map';
import Streams                     from './pages/Streams';
import Hosts                       from './pages/Hosts';
import System                      from './pages/System';
import NotFoundPage                from './pages/NotFoundPage';

export default (
  <Router history={CreateBrowserHistory()}>

    <Route path="/" component={App}>
      <IndexRoute path="dashboard" component={Dashboard} />
      <Route path="dashboard" component={Dashboard}  />
      <Route path="map" component={Map} />
      <Route path="streams" component={Streams} />
      <Route path="hosts" component={Hosts} />
      <Route path="system" component={System} />
      <Route path="*" component={NotFoundPage} />
    </Route>

  </Router>
);
