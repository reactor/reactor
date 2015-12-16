'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyIndex', function () {

    var stream = gulp.src([config.sourceDir + 'index.html', config.sourceDir +'index.appcache']);

    if (!global.isProd && config.devDir !== undefined) {
        stream.pipe(gulp.dest(config.devDir));
    }

    return stream.pipe(gulp.dest(config.buildDir));

});