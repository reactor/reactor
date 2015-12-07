'use strict';

const config = {
  browserPort: 3000,
  browser: false,
  UIPort: 3001,
  scripts: {
    src: './js/**/*.js',
    dest: './src/main/resources/public/assets/js/'
  },
  images: {
    src: './src/main/static/images/**/*.{jpeg,jpg,png,gif}',
    dest: './src/main/resources/public/assets/images/'
  },
  styles: {
    src: './src/main/static/styles/main.scss',
    dest: './src/main/resources/public/assets/css/',
    watch: './src/main/static/styles/**.scss'
  },
  sourceDir: './src/main/static/',
  buildDir: './src/main/resources/public/',
  devDir: './build/resources/main/public/'
};

export default config;