'use strict';

const config = {
  browserPort: 3000,
  UIPort: 3001,
  scripts: {
    src: './js/**/*.js',
    dest: './src/main/resources/public/js/'
  },
  images: {
    src: './src/main/static/images/**/*.{jpeg,jpg,png,gif}',
    dest: './src/main/resources/public/images/'
  },
  styles: {
    src: './src/main/static/styles/main.scss',
    dest: './src/main/resources/public/css/'
  },
  sourceDir: './src/main/static/',
  buildDir: './src/main/resources/public/'
};

export default config;