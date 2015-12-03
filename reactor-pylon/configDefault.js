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
module.exports = {
    build: {
        vendor: {
            js: [
                //'./public/src/js/code.js',
                './node_modules/es5-shim/es5-shim.js',
                './node_modules/jquery/dist/jquery.js',
                './node_modules/vis/dist/vis.min.js'
                // './node_modules/underscore/underscore.min.js',
            ],
            css: [
                './node_modules/vis/dist/vis.min.css'
            ],
            map: [
                './node_modules/vis/dist/vis.map'
            ]
        },
        classpathSource: './src/main/static/',
        classpathTarget: './src/main/resources/public/',
        classpathDevTarget: './build/resources/main/public/',
        browserify: {
            paths: [
                './src/main/static',
                './node_modules'
            ],
            debug: true
        },
        clean: {
            copy: ['./src/main/resources/public/assets', './src/main/resources/public/**.html'],
            scripts: ['./src/main/resources/public/js/**.*'],
            styles: ['./src/main/resources/public/css/**.*'],
            fonts: ['./src/main/resources/public/fonts/**.*']
        },
        cssmin: false,
        uglify: false
    }
};