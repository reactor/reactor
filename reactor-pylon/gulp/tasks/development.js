'use strict';

import gulp        from 'gulp';
import runSequence from 'run-sequence';

gulp.task('dev', ['clean'], function(cb) {

  cb = cb || function() {};

  global.isProd = false;

  // Run all tasks once
  return runSequence(['sass', 'imagemin', 'browserify', 'copyFonts', 'copyFontAwesome', 'copyIndex', 'copyIcons'], 'watch', cb);

});