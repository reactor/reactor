'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyFonts', function () {

    var stream = gulp.src([config.sourceDir + 'fonts/**/*']);

    if (!global.isProd && config.devDir !== undefined) {
        stream.pipe(gulp.dest(config.devDir + 'assets/fonts'));
    }

    return stream.pipe(gulp.dest(config.buildDir + 'assets/fonts/'));
});
