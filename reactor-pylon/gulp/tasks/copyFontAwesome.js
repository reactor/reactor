'use strict';

import gulp   from 'gulp';
import config from '../config';

gulp.task('copyFontAwesome', function() {

  return gulp.src(['./node_modules/font-awesome/fonts/**.*'], { base: './node_modules/font-awesome/' })
    .pipe(gulp.dest(config.buildDir));

});
