/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/**
 Algorithm taken from: https://blog.cedric.ws/draw-the-convex-hull-with-canvas-and-javascript
 **/

export default ConvexHull;

// Point class
export class Point {
    constructor(x, y) {
        this.x = x;
        this.y = y;
    }

    toString() {
        return "x: " + this.x + ", y: " + this.y;
    }

    rotateRight(p1, p2) {
        // cross product, + is counterclockwise, - is clockwise
        return ((p2.x * this.y - p2.y * this.x) - (p1.x * this.y - p1.y * this.x) + (p1.x * p2.y - p1.y * p2.x)) < 0;
    }
}

// ConvexHull class
export class ConvexHull {

    constructor(points) {
        this.hull = null;
        this.points = points;
    }

    calculate() {
        this.hull = [];
        this.points.sort(function compare(p1, p2) {
            return p1.x - p2.x;
        });

        var upperHull = [];
        this.calcUpperhull(upperHull);
        var i;
        for (i = 0; i < upperHull.length; i++) {
            this.hull.push(upperHull[i]);
        }

        var lowerHull = [];
        this.calcLowerhull(lowerHull);
        for (i = 0; i < lowerHull.length; i++) {
            this.hull.push(lowerHull[i]);
        }
    }

    calcUpperhull(upperHull) {
        var i = 0;
        upperHull.push(this.points[i]);
        i++;
        upperHull.push(this.points[i]);
        i++;
        // Start upperHull scan
        for (i; i < this.points.length; i++) {
            upperHull.push(this.points[i]);
            while (upperHull.length > 2 && // more than 2 points
            !upperHull[upperHull.length - 3].rotateRight(upperHull[upperHull.length - 1],
                upperHull[upperHull.length - 2]) // last 3 points make left turn
                ) {
                upperHull.splice(upperHull.indexOf(upperHull[upperHull.length - 2]), 1);
            } // remove middle point
        }
    }

    calcLowerhull(lowerHull) {
        var i = this.points.length - 1;
        lowerHull.push(this.points[i]);
        i--;
        lowerHull.push(this.points[i]);
        i--;
        // Start lowerHull scan
        for (i; i >= 0; i--) {
            lowerHull.push(this.points[i]);
            while (lowerHull.length > 2 && // more than 2 points
            !lowerHull[lowerHull.length - 3].rotateRight(lowerHull[lowerHull.length - 1],
                lowerHull[lowerHull.length - 2]) // last 3 points make left turn
                ) {
                lowerHull.splice(lowerHull.indexOf(lowerHull[lowerHull.length - 2]), 1);
            } // remove middle point
        }
    }
}