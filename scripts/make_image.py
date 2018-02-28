#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
import sys
import os
import argparse
import logging

from run_command import run_command


# Path to the fits2png.x executable from the SPoCA software suite
fits2png_bin = '/home/sdo/SPoCA/bin/fits2png.x'

# Path to the convert executable from the ImageMagick software suite
convert_bin = 'convert'

def fits_to_png(input_filename, output_directory, size=None):
	# We set up fits2png for the creation of the png
	fits2png = [fits2png_bin, input_filename, '-u', '-R', '512.5,512.5', '-L', '-c']
	if size:
		fits2png.extend(['-S', size])
	
	fits2png.extend(['-O', output_directory])
	
	return run_command(fits2png)


def image_to_thumbnail(input_filename, output_filename, size):
	# We set up convert for the creation of the thumbnail
	convert = [convert_bin, input_filename, '-resize', size, output_filename]
	
	return run_command(convert)


def image_to_button(input_filename, output_filename, size):
	# We set up convert for the creation of the button
	convert = [convert_bin, input_filename, '-resize', size, '-fuzz', '10%', '-transparent', 'black', output_filename]
	
	return run_command(convert)


# Start point of the script
if __name__ == '__main__':
	
	# Get the arguments
	parser = argparse.ArgumentParser(description='Convert a solar fits file to a png image')
	parser.add_argument('--debug', '-d', default=False, action='store_true', help='Set the logging level to debug')
	parser.add_argument('--verbose', '-v', default=False, action='store_true', help='Set the logging level to info')
	parser.add_argument('--overwrite', '-o', default=False, action='store_true', help='Overwrite the image if it already exists')
	parser.add_argument('--image_size', '-s', default=None, help='The size of the iamge. Must be specified like widthxheight in pixels')
	parser.add_argument('--image_filename', '-f', required=True, help='The filename for the image, must end in .png')
	parser.add_argument('source', help='The paths of the source fits file')
	
	args = parser.parse_args()
	
	# Setup the logging
	if args.debug:
		logging.basicConfig(level = logging.DEBUG, format='%(levelname)-8s: %(message)s')
	elif args.verbose:
		logging.basicConfig(level = logging.INFO, format='%(levelname)-8s: %(message)s')
	else:
		logging.basicConfig(level = logging.ERROR, format='%(levelname)-8s: %(message)s')
	
	if os.path.exists(args.image_filename):
		if not args.overwrite:
			logging.error('Image %s already exists, not overwriting', args.image_filename)
			sys.exit(1)
		else:
			logging.info('Image %s will be overwritten', args.image_filename)
	
	image_path, image_extension = os.path.splitext(args.image_filename)
	
	if image_extension == '.png':
		logging.info('Making png image %s', args.image_filename)
		fits_to_png(args.source, args.image_filename, size = args.image_size)
	else:
		logging.critical('Image filename must end in .png')
		sys.exit(2)
