'use strict';

import {camelizeKeys} from 'humps';
import request        from 'superagent';
import RxDOM           from 'rx-dom';
import Rx           from 'rx-lite';
import JSON           from 'JSON2';
import cookie           from 'react-cookie';

const APIUtils = {

    defaultTarget: 'localhost:12012/nexus/', target: '',

    offline: 0, ready: 1, working: 2, retry: 3,

    defaultOrLastTarget() {
        var target = cookie.load('targetAPI');
        if (typeof target === 'undefined') {
            this.target = this.defaultTarget;
            console.log("updating targetAPI cookie with : " + this.target)
            cookie.save('targetAPI', this.target);
        }
        else {
            this.target = target;
        }
        return this.target;
    },

    updateTargetAPI(target) {
        if (typeof target !== 'undefined') {
            this.target = target;
            cookie.save('targetAPI', this.target);
        }
    },

    normalizeResponse(response) {
        return camelizeKeys(response.body);
    },

    get(path) {
        return new Promise((resolve, reject) => {
            request.get(this.target + path)
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
            request.post(this.target + path, body)
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
            request.patch(this.target + path, body)
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
            request.put(this.target + path, body)
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
            request.del(this.target + path)
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

    ws(path, stateObserver) {
        // Handle the data
        return new Promise((resolve, reject) => {

            if (stateObserver !== undefined) {
                stateObserver.onNext(this.working);
            }
            var ws = new WebSocket("ws://" + this.target + path);

            var thiz = this;
            ws.onopen = (e) => {
                if (stateObserver !== undefined) {
                    stateObserver.onNext(thiz.ready);
                }

                resolve({
                    receiver: Rx.Observable.create ((obs) => {
                        // Handle messages
                        if (ws == null) {
                            ws = new WebSocket("ws://" + thiz.target + path);
                            ws.onopen = (e) => {
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.ready);
                                }
                            };
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
                            if (stateObserver !== undefined) {
                                stateObserver.onNext(thiz.offline);
                            }
                            obs.onError(e);
                        };

                        ws.onclose = (e) => {
                            if (ws != null) {
                                ws = null;
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.offline);
                                }
                                obs.onError({message: 'Server connection has been lost'});
                            }
                        };

                        // Return way to unsubscribe
                        return () => {
                            if (ws != null) {
                                console.log("closing connection");
                                var _ws = ws;
                                ws = null;
                                _ws.close();
                            }
                        }
                    }).retryWhen(attempts => {
                        return Rx.Observable
                            .range(1, 10)
                            .zip(attempts, i => i)
                            .scan(i => i * 2, 1)
                            .flatMap(i => {
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.retry);
                                }
                                console.log("delay retry by " + i + " second(s)");
                                return Rx.Observable.timer(i * 1000);
                            });
                    }), sender: Rx.Observer.create((data) => {
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