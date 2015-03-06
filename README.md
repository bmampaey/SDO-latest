SDO latest website
===================

This repository gather the differents parts of the SDO latest website

website
-------
All the html and css for the website. Should be installed in /var/www.html

apache
------
The apache configuration file. Should be installed in /etc/apache2/sites-available, and a symbolic link pointing to it should be created in /etc/apache2/sites-enabled

scripts
-------
The scripts to generate the images and videos.
Requires ffmpeg to be compiled with the x264 library
Requires fits2png.x from the SPoCA software to be compile with the image magick Magick++ library. (Use the correct version as some have a bug in it)
