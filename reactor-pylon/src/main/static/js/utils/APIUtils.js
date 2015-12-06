'use strict';

import {camelizeKeys} from 'humps';
import request        from 'superagent';
import RxDOM           from 'rx-dom';
import Rx           from 'rx';

const APIUtils = {

  root: '//localhost:12012/nexus/',

  normalizeResponse(response) {
    return camelizeKeys(response.body);
  },

  get(path) {
    return new Promise((resolve, reject) => {
      request.get(this.root + path)
      .withCredentials()
      .end((err, res) => {
        if ( err || !res.ok ) {
          reject(this.normalizeResponse(err || res));
        } else {
          resolve(this.normalizeResponse(res));
        }
      });
    });
  },

  post(path, body) {
    return new Promise((resolve, reject) => {
      request.post(this.root + path, body)
      .withCredentials()
      .end((err, res) => {
        console.log(err, res);
        if ( err || !res.ok ) {
          reject(this.normalizeResponse(err || res));
        } else {
          resolve(this.normalizeResponse(res));
        }
      });
    });
  },

  patch(path, body) {
    return new Promise((resolve, reject) => {
      request.patch(this.root + path, body)
      .withCredentials()
      .end((err, res) => {
        if ( err || !res.ok ) {
          reject(this.normalizeResponse(err || res));
        } else {
          resolve(this.normalizeResponse(res));
        }
      });
    });
  },

  put(path, body) {
    return new Promise((resolve, reject) => {
      request.put(this.root + path, body)
      .withCredentials()
      .end((err, res) => {
        if ( err || !res.ok ) {
          reject(this.normalizeResponse(err || res));
        } else {
          resolve(this.normalizeResponse(res));
        }
      });
    });
  },

  del(path) {
    return new Promise((resolve, reject) => {
      request.del(this.root + path)
      .withCredentials()
      .end((err, res) => {
        if ( err || !res.ok ) {
          reject(this.normalizeResponse(err || res));
        } else {
          resolve(this.normalizeResponse(res));
        }
      });
    });
  },

  ws(path) {
    /* var ws = new WebSocket(this.root + path);

     var observer = Rx.Observer.create(function (data) {
     if (ws.readyState === WebSocket.OPEN) { ws.send(data); }
     });

     // Handle the data
     var observable = Rx.Observable.create (function (obs) {
     // Handle open
     if (openObserver) {
     ws.onopen = function (e) {
     openObserver.onNext(e);
     openObserver.onCompleted();
     };
     }

     // Handle messages
     ws.onmessage = function(msg){
     var data =  angular.fromJson(msg.data);
     if(data.cause){
     obs.onError(data);
     }else{
     obs.onNext(data);
     }
     };
     ws.onerror = function(e){
     var error = {cause: e.message};
     handleError(error);
     obs.onError(error);
     };

     ws.onclose = function(e){
     obs.onCompleted();
     };

     // Return way to unsubscribe
     return function(){
     console.log("closing connection");
     ws.close();
     socketStream = null;
     }
     });

     return Rx.Subject.create(observer, observable);

     return new Promise((resolve, reject) => {
     request.del(this.root + path)
     .withCredentials()
     .end((err, res) => {
     if ( err || !res.ok ) {
     reject(this.normalizeResponse(err || res));
     } else {
     resolve(this.normalizeResponse(res));
     }
     });
     });*/
  }
};

export default APIUtils;