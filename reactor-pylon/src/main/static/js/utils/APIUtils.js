'use strict';

import {camelizeKeys} from 'humps';
import request        from 'superagent';
import RxDOM           from 'rx-dom';
import Rx           from 'rx-lite';
import JSON           from 'JSON2';

const APIUtils = {

    root: 'localhost:12012/nexus/',

    normalizeResponse(response) {
        return camelizeKeys(response.body);
    },

    get(path) {
        return new Promise((resolve, reject) => {
            request.get(this.root + path)
                .withCredentials()
                .end((err, res) => {
                    if (err || !res.ok) {
                        reject(this.normalizeResponse(err || res));
                    }
                    else {
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
                    if (err || !res.ok) {
                        reject(this.normalizeResponse(err || res));
                    }
                    else {
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
                    if (err || !res.ok) {
                        reject(this.normalizeResponse(err || res));
                    }
                    else {
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
                    if (err || !res.ok) {
                        reject(this.normalizeResponse(err || res));
                    }
                    else {
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
                    if (err || !res.ok) {
                        reject(this.normalizeResponse(err || res));
                    }
                    else {
                        resolve(this.normalizeResponse(res));
                    }
                });
        });
    },

    ws(path, open) {
        // Handle the data
        return new Promise((resolve, reject) => {
            var ws = new WebSocket("ws://"+this.root + path);

            ws.onopen = (e) => {
                if (open !== 'undefined') {
                    open(e);
                }

                resolve({
                    receiver: Rx.Observable.create ((obs) => {
                        // Handle messages
                        if(ws == null){
                            ws = new WebSocket("ws://"+this.root + path);
                        }
                        ws.onmessage = (msg) => {
                            var data = JSON.parse(msg.data);
                            if (data.cause) {
                                obs.onError(data);
                            }
                            else {
                                obs.onNext(data);
                            }
                        };
                        ws.onerror = (e) => {
                            ws = null;
                            obs.onError(e);
                        };

                        ws.onclose = (e) => {
                            ws = null;
                            obs.onError({message:'Server connection has been lost'});
                        };

                        // Return way to unsubscribe
                        return () => {
                            if(ws != null) {
                                console.log("closing connection");
                                ws.close();
                            }
                        }
                    }).retryWhen(attempts => {
                        return Rx.Observable
                            .range(1, 10)
                            .zip(attempts, i => i)
                            .scan(1, i => i * 2)
                            .flatMap(i => {
                                console.log("delay retry by " + i + " second(s)");
                                return Rx.Observable.timer(i * 1000);
                            });
                    })
                    , sender: Rx.Observer.create((data) => {
                        if (ws.readyState === WebSocket.OPEN) {
                            ws.send(data);
                        }
                    })
                });
            };

            ws.onerror = reject;
        });
    }
};

export default APIUtils;