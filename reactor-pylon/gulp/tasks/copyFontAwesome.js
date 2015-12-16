'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyFontAwesome', function () {

    var stream = gulp.src(['./node_modules/font-awesome/fonts/**.*'], {base: './node_modules/font-awesome/'});

    if (!global.isProd && config.devDir !== undefined) {
        stream.pipe(gulp.dest(config.devDir + 'assets'));
    }

    return stream.pipe(gulp.dest(config.buildDir + 'assets'));
});
