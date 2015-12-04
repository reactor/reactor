'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyIndex', function () {

    return gulp.src(config.sourceDir + 'index.html')
        .pipe(gulp.dest(config.buildDir))
        .pipe(gulpif(config.devDir !== 'undefined', gulp.dest(config.devDir)))

});