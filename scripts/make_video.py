#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
import sys
import os
import string
import logging
import argparse

from run_command import run_command, run_command_with_input_files

# Path to ffmpeg with libx264 compiled in
ffmpeg_bin = '/home/sdo/ffmpeg/bin/ffmpeg'

def png_to_mp4_video(input_filenames, output_filename, frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We set up ffmpeg for the creation of mp4
	ffmpeg = [ffmpeg_bin, '-y', '-r', str(frame_rate), '-f', 'image2pipe', '-vcodec', 'png', '-i', '-', '-an', '-vcodec', 'libx264', '-preset', 'slow', '-vprofile', 'baseline', '-pix_fmt', 'yuv420p']
	
	if video_bitrate:
		ffmpeg.extend(['-maxrate', str(video_bitrate) + 'k'])
	
	if video_title:
		ffmpeg.extend(['-metadata', 'title=' + str(video_title)])
	
	if video_size:
		ffmpeg.extend(['-s', video_size])
	
	ffmpeg.append(output_filename)
	
	return run_command_with_input_files(ffmpeg, input_filenames)

def png_to_ts_video(input_filenames, output_filename, frame_rate = 24, video_title = None, video_size = None, video_bitrate = None, video_preset='ultrafast'):
	
	# We set up ffmpeg for the creation of ts
	ffmpeg = [ffmpeg_bin, '-y', '-loglevel', 'debug', '-r', str(frame_rate), '-f', 'image2pipe', '-vcodec', 'png', '-i', '-', '-an', '-vcodec', 'libx264', '-preset', video_preset, '-qp', '0']
	
	if video_bitrate:
		ffmpeg.extend(['-maxrate', str(video_bitrate) + 'k'])
	
	if video_title:
		ffmpeg.extend(['-metadata', 'title=' + str(video_title)])
	
	if video_size:
		ffmpeg.extend(['-s', video_size])
	
	ffmpeg.append(output_filename)
	
	return run_command_with_input_files(ffmpeg, input_filenames)


def video_to_mp4_video(input_filenames, output_filename, frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We set up ffmpeg for the creation of mp4
	ffmpeg = [ffmpeg_bin, '-y', '-i']
	
	if isinstance(input_filenames, basestring):
		ffmpeg.append(input_filenames)
	elif len(input_filenames) == 1:
		ffmpeg.append(input_filenames[0])
	else:
		ffmpeg.append('concat:'+'|'.join(input_filenames))
	
	ffmpeg.extend(['-an', '-vcodec', 'libx264', '-preset', 'slow', '-vprofile', 'baseline', '-pix_fmt', 'yuv420p', '-r', str(frame_rate)])
	
	if video_bitrate:
		ffmpeg.extend(['-maxrate', str(video_bitrate) + 'k'])
	
	if video_title:
		ffmpeg.extend(['-metadata', 'title=' + str(video_title)])
	
	if video_size:
		ffmpeg.extend(['-s', video_size])
	
	ffmpeg.append(output_filename)
	
	return run_command(ffmpeg)

def video_to_webm_video(input_filenames, output_filename, frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We set up ffmpeg for the creation of webm
	ffmpeg = [ffmpeg_bin, '-y', '-i']
	
	if isinstance(input_filenames, basestring):
		ffmpeg.append(input_filenames)
	elif len(input_filenames) == 1:
		ffmpeg.append(input_filenames[0])
	else:
		ffmpeg.append('concat:'+'|'.join(input_filenames))
	
	ffmpeg.extend(['-an', '-vcodec', 'libvpx', '-cpu-used', '0', '-qmin', '10', '-qmax', '42', '-threads', '2', '-r', str(frame_rate)])
	
	if video_bitrate:
		ffmpeg.extend(['-maxrate', str(video_bitrate) + 'k'])
	
	if video_title:
		ffmpeg.extend(['-metadata', 'title=' + str(video_title)])
	
	if video_size:
		ffmpeg.extend(['-s', video_size])
	
	ffmpeg.append(output_filename)
	
	return run_command(ffmpeg)

def video_to_ogv_video(input_filenames, output_filename, frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We set up ffmpeg for the creation of ogv
	ffmpeg = [ffmpeg_bin, '-y', '-i']
	if isinstance(input_filenames, basestring):
		ffmpeg.append(input_filenames)
	elif len(input_filenames) == 1:
		ffmpeg.append(input_filenames[0])
	else:
		ffmpeg.append('concat:'+'|'.join(input_filenames))
	
	ffmpeg.extend(['-an', '-vcodec', 'libtheora', '-q:v', '7', '-r', str(frame_rate)])
	
	if video_bitrate:
		ffmpeg.extend(['-maxrate', str(video_bitrate) + 'k'])
	
	if video_title:
		ffmpeg.extend(['-metadata', 'title=' + str(video_title)])
	
	if video_size:
		ffmpeg.extend(['-s', video_size])
	
	ffmpeg.append(output_filename)
	
	return run_command(ffmpeg)

# Start point of the script
if __name__ == '__main__':
	
	# Get the arguments
	parser = argparse.ArgumentParser(description='Make mp4 video from png.')
	parser.add_argument('--debug', '-d', default=False, action='store_true', help='Set the logging level to debug')
	parser.add_argument('--verbose', '-v', default=False, action='store_true', help='Set the logging level to info')
	parser.add_argument('--overwrite', '-o', default=False, action='store_true', help='Overwrite the video if it already exists')
	parser.add_argument('--frame_rate', '-r', default=24, type=float, help='Frame rate for the video')
	parser.add_argument('--video_bitrate', '-b', default=0, type=float, help='A maximal bitrate for the video in kb')
	parser.add_argument('--video_title', '-t', default=None, help='A title for the video')
	parser.add_argument('--video_size', '-s', default=None, help='The size of the video. Must be specified like widthxheight in pixels')
	parser.add_argument('--video_filename', '-f', required=True, help='The filename for the video, must end in .mp4 or .ts')
	parser.add_argument('sources', nargs='+', help='The paths of the source png images')
	
	args = parser.parse_args()
	
	# Setup the logging
	if args.debug:
		logging.basicConfig(level = logging.DEBUG, format='%(levelname)-8s: %(message)s')
	elif args.verbose:
		logging.basicConfig(level = logging.INFO, format='%(levelname)-8s: %(message)s')
	else:
		logging.basicConfig(level = logging.ERROR, format='%(levelname)-8s: %(message)s')
	
	if os.path.exists(args.video_filename):
		if not args.overwrite:
			logging.error('Video %s already exists, not overwriting', args.video_filename)
			sys.exit(1)
		else:
			logging.info('Video %s will be overwritten', args.video_filename)
	
	video_path, video_extension = os.path.splitext(args.video_filename)
	
	if video_extension == '.mp4':
		logging.info('Making mp4 video %s', args.video_filename)
		png_to_mp4_video(args.sources, args.video_filename, frame_rate = args.frame_rate, video_title = args.video_title, video_size = args.video_size, video_bitrate = args.video_bitrate)
	elif video_extension == '.ts':
		logging.info('Making ts video %s', args.video_filename)
		png_to_ts_video(args.sources, args.video_filename, frame_rate = args.frame_rate, video_title = args.video_title, video_size = args.video_size, video_bitrate = args.video_bitrate)
	else:
		logging.critical('Video filename must end in .mp4 or .ts')
		sys.exit(2)
