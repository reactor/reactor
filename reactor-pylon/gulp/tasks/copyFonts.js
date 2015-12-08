'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyFonts', function () {

    return gulp.src([config.sourceDir + 'fonts/**/*'])
        .pipe(gulp.dest(config.buildDir + 'assets/fonts/'))
        .pipe(gulpif(typeof config.devDir !== 'undefined', gulp.dest(config.devDir + 'assets/fonts')));
});
