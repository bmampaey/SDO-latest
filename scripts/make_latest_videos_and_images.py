#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
'''
Deamon to generate images and videos from aia quicklook fits files for the sdodata latest website
'''

import sys, os, errno, glob
import logging
import argparse
import shutil
import signal
from time import sleep
from datetime import time, datetime, timedelta
from dateutil.parser import parse as parse_date
import threading
import Queue
import pyfits

from make_video import png_to_ts_video, video_to_mp4_video
from make_image import fits_to_png, image_to_thumbnail, image_to_button

# Max number of concurrent threads
max_threads = 5

# Duration in hours to go back in time for the creation of images and videos
time_span = 3 * 24

# Maximum run frequency in seconds per function
max_run_frequency = {
	'make_images': timedelta(minutes = 5),
	'make_latest_images': timedelta(minutes = 5),
	'make_video_pieces': timedelta(minutes = 10),
	'make_latest_videos': timedelta(minutes = 10),
	'make_daily_videos': timedelta(hours = 12),
}

# Min acceptable AIA quality bits (See AIA/SDO keywords)
AIA_min_quality = (1 << 2) + (1 << 8) + (1 << 9) + (1 << 13) + (1 << 30)

# AIA Fits files wavelengths
AIA_wavelengths = [94, 131, 171, 193, 211, 304, 335, 1600, 1700, 4500]

# Paths of the fits files
fitsfiles_directory = '/data/SDO/public/AIA_quicklook/{wavelength:04d}/{date.year:04d}/{date.month:02d}/{date.day:02d}/H{date.hour:02d}00/'

# Paths of the images
images_directory_pattern = '/data/SDO/public/latest/images/{date.year:04d}/{date.month:02d}/{date.day:02d}/H{date.hour:02d}00/'
latest_image_pattern = '/data/SDO/public/latest/images/latest/AIA.latest.{wavelength:04d}.quicklook.{suffix}'

# Paths of the videos
video_piece_pattern = '/data/SDO/public/latest/videos_pieces/{date.year:04d}/{date.month:02d}/{date.day:02d}/H{date.hour:02d}00/AIA.{date.year:04d}{date.month:02d}{date.day:02d}_{date.hour:02d}0000.{wavelength:04d}.quicklook.ts'
daily_video_pattern = '/data/SDO/public/latest/videos/{date.year:04d}/{date.month:02d}/{date.day:02d}/AIA.{date.year:04d}{date.month:02d}{date.day:02d}_{date.hour:02d}0000.{wavelength:04d}.quicklook.{suffix}'
latest_video_pattern = '/data/SDO/public/latest/videos/latest/AIA.latest.{wavelength:04d}.quicklook.{suffix}'

# Parameters for images
image_large_size = '1024x1024>'
image_medium_size = '128x128>'
image_small_size = '45x45>'

# Parameters for videos
video_frame_rate = 16

# Duration in hours of the latest videos per wavelength
latest_video_length = dict.fromkeys(AIA_wavelengths, 24)
latest_video_length[4500] = 24 * 20

class SharedCache(object):
	def __init__(self):
		self.lock = threading.Lock()
		self.cache = dict()
	
	def add(self, item):
		self.lock.acquire()
		self.cache[item] = datetime.now()
		self.lock.release()
	
	def __contains__(self, item):
		return item in self.cache
	
	def clean(self, age):
		now = datetime.now()
		self.lock.acquire()
		for key, value in self.cache.iteritems():
			if value + age < now:
				del self.cache[key]
		self.lock.release()

def make_directory(directory):
	'''Create a directory and all the subdirectories'''
	try:
		os.makedirs(directory)
	except OSError, why:
		if why.errno == errno.EEXIST:
			pass
		else:
			logging.critical('Cannot create directory %s: %s', directory, why)
			raise

def round_to_hour(date):
	return date.replace(minute=0, second=0, microsecond=0)

def get_keywords(fitsfile, keywords):
	result = dict.fromkeys(keywords)
	
	hdulist = pyfits.open(fitsfile)
	for hdu in hdulist:
		for keyword in keywords:
			if keyword in hdu.header:
				result[keyword] = hdu.header[keyword]
		
	hdulist.close()
	
	return result

def get_daily_video_dates(date):
	# There is one video starting at midnight, and one at noon
	day = date.replace(hour=0, minute=0, second=0, microsecond=0)
	
	if date.time() < time(hour=12):
		return day - timedelta(hours=12), day
	else:
		return day + timedelta(hours=12), day + timedelta(hours=24)

def terminate_gracefully(signal, frame):
	logging.info('Received signal %s: Exiting gracefully', signal)
	stop_daemon.set()
	sys.exit(0)

def run_threads(target, args=(), kwargs={}):
	threads = list()
	
	# Start the threads
	for i in range(max_threads):
		name = target.__name__ + ('_%02d' % i)
		logging.debug('Starting thread %s with args:\n%s\n%s', name, args, kwargs)
		thread = threading.Thread(name=name, target=target, args=args, kwargs=kwargs)
		thread.daemon = True
		thread.start()
		threads.append(thread)
	
	# Wait for the threads to terminate
	for thread in threads:
		thread.join()

def make_images():
	
	input_queue = Queue.Queue()
	output_queue = Queue.Queue()
	
	# Start date of images
	date = round_to_hour(datetime.utcnow()) - timedelta(hours = time_span)
	
	# Add the fitsfiles to the input queue
	for wavelength in AIA_wavelengths:
		for hours in range(time_span + 1):
			directory_path = fitsfiles_directory.format(date=date + timedelta(hours = hours), wavelength=wavelength)
			logging.debug('Getting fits files for directory %s', directory_path)
			for fitsfile in glob.glob(os.path.join(directory_path, '*.fits')):
				input_queue.put(fitsfile)
	
	# Make the images in parralel threads
	run_threads(target=thread_make_images, kwargs={'input_queue': input_queue, 'output_queue': output_queue})
	
	# Extract the images from the output queue
	images = list()
	while not output_queue.empty():
		try:
			images.append(output_queue.get_nowait())
		except Queue.Empty:
			continue
	
	return images

def thread_make_images(input_queue, output_queue):
	
	while not input_queue.empty() and not stop_daemon.is_set():
		
		try:
			fitsfile = input_queue.get_nowait()
		except Queue.Empty:
			continue
		
		if fitsfile in bad_fitsfiles:
			logging.info('Fits file %s in list of bad fits files, skipping!', fitsfile)
			continue
		
		# We get the necessary keywords
		try:
			keywords = get_keywords(fitsfile, ['DATE-OBS', 'WAVELNTH', 'QUALITY'])
		except Exception, why:
			logging.error('Error reading keywords from file %s: %s, skipping!', fitsfile, why)
			continue
		
		# We check the date
		try:
			date_obs = parse_date(keywords['DATE-OBS'])
		except Exception, why:
			logging.warning('DATE-OBS keyword in file %s (%s) is invalid: %s, skipping!', fitsfile, keywords['DATE-OBS'], why)
			continue
		
		# We check if the file already exists
		image_directory = images_directory_pattern.format(date=date_obs)
		image_path = os.path.join(image_directory, os.path.splitext(os.path.basename(fitsfile))[0]+ '.png')
		if os.path.isfile(image_path):
			logging.debug('Fits file %s already converted to image %s, skipping!', fitsfile, image_path)
			continue
		
		# We check the wavelength
		try:
			wavelength = int(keywords['WAVELNTH'])
		except Exception, why:
			logging.warning('WAVELNTH keyword in file %s (%s) is invalid, skipping!', fitsfile, keywords['WAVELNTH'])
			continue
		
		if wavelength not in AIA_wavelengths:
			logging.warning('Unknown wavelength %s for file %s, skipping!', wavelength, fitsfile)
			continue
		
		# We check the quality
		try:
			quality = int(keywords['QUALITY'])
		except Exception, why:
			logging.warning('QUALITY keyword in file %s (%s) is invalid, skipping!', fitsfile, keywords['QUALITY'])
			continue
		
		if quality | AIA_min_quality != AIA_min_quality:
			logging.warning('Quality of file %s (%s) does not meet the minimum required quality, skipping!', fitsfile, quality)
			logging.debug('Adding fitsfile %s to list of bad fitsfiles', fitsfile)
			bad_fitsfiles.add(fitsfile)
			continue
		
		# We make the image directory
		make_directory(image_directory)
		
		# We make the image
		logging.info('Making image for file %s', fitsfile)
		if fits_to_png(fitsfile, image_directory):
			output_queue.put({'date': date_obs, 'wavelength': wavelength, 'path': image_path})
		
		else:
			logging.error('Error while making image from file %s', fitsfile)


def make_latest_images(latest_images_to_make):
	
	if latest_images_to_make:
		
		input_queue = Queue.Queue()
		
		# Add the latest images to the input queue
		for latest_image in latest_images_to_make:
			input_queue.put(latest_image)
		
		# Make the latest images in parralel threads
		run_threads(target=thread_make_latest_images, kwargs={'input_queue': input_queue})
	
	else:
		logging.debug('No latest image to make')

def thread_make_latest_images(input_queue):
	
	while not input_queue.empty() and not stop_daemon.is_set():
		
		try:
			image = input_queue.get_nowait()
		except Queue.Empty:
			continue
		
		latest_image_path = latest_image_pattern.format(wavelength=image['wavelength'], suffix='large.png')
		
		logging.debug('Copying %s to %s', image['path'], latest_image_path)
		try:
			make_directory(os.path.dirname(latest_image_path))
			shutil.copy(image['path'], latest_image_path)
		except Exception, why:
			logging.error('Error copying %s to %s: %s', image['path'], latest_image_path, why)
		else:
			# We use the large image to create the corresponding thumbnails
			image_to_thumbnail(latest_image_path, latest_image_pattern.format(wavelength=image['wavelength'], suffix='small.png'), image_small_size)
			image_to_thumbnail(latest_image_path, latest_image_pattern.format(wavelength=image['wavelength'], suffix='medium.png'), image_medium_size)
			image_to_button(latest_image_path, latest_image_pattern.format(wavelength=image['wavelength'], suffix='button.png'), image_medium_size)


def make_video_pieces(video_pieces_to_make):
	
	# Start date of video pieces
	date = round_to_hour(datetime.utcnow()) - timedelta(hours = time_span)
	
	# Add the missing videos pieces
	for wavelength in AIA_wavelengths:
		for hours in range(time_span + 1):
			video_path = video_piece_pattern.format(date = date + timedelta(hours = hours), wavelength = wavelength)
			if not os.path.exists(video_path):
				logging.info('Video piece %s is missing, will be made', video_path)
				video_pieces_to_make.add((wavelength, date + timedelta(hours = hours)))
	
	if video_pieces_to_make:
		
		input_queue = Queue.Queue()
		output_queue = Queue.Queue()
		
		# Add the videos pieces to the input queue
		for video_piece in video_pieces_to_make:
			input_queue.put(video_piece)
		
		# Make the videos pieces in parralel threads
		run_threads(target=thread_make_video_pieces, kwargs={'input_queue': input_queue, 'output_queue': output_queue, 'video_frame_rate': video_frame_rate})
		
		# Extract the videos pieces from the output queue
		video_pieces = list()
		while not output_queue.empty():
			try:
				video_pieces.append(output_queue.get_nowait())
			except Queue.Empty:
				continue
		
		return video_pieces
	
	else:
		logging.debug('No video piece to make')
		return []


def thread_make_video_pieces(input_queue, output_queue, video_frame_rate = 24, video_size = None, video_bitrate = None, video_title = None):
	
	while not input_queue.empty() and not stop_daemon.is_set():
		
		try:
			wavelength, date = input_queue.get_nowait()
		except Queue.Empty:
			continue
		
		# We make the list of frames
		images_directory = images_directory_pattern.format(date=date)
		images = sorted(glob.glob(os.path.join(images_directory, '*%04d.quicklook.png' % wavelength)))
		
		if not images:
			logging.warning('No images found to make video piece for date %s and wavelength %d, skipping!', date, wavelength)
			continue
		
		video_path = video_piece_pattern.format(date=date, wavelength=wavelength)
		make_directory(os.path.dirname(video_path))
		
		# We make the video piece
		if png_to_ts_video(images, video_path, frame_rate = video_frame_rate, video_title = video_title, video_size = video_size, video_bitrate = video_bitrate, video_preset='slow'):
			output_queue.put({'wavelength': wavelength, 'date': date, 'video_path': video_path})
		else:
			logging.error('Error while making video piece for date %s and wavelength %d', date, wavelength)

def make_latest_videos(latest_videos_to_make):
	
	# Add the missing latest videos
	for wavelength in AIA_wavelengths:
		latest_video_path = latest_video_pattern.format(wavelength=wavelength, suffix='mp4')
		if not os.path.exists(latest_video_path):
			logging.info('Latest video %s is missing, will be made', latest_video_path)
			latest_videos_to_make.add(wavelength)
	
	if latest_videos_to_make:
		
		input_queue = Queue.Queue()
		
		# Add the latest videos to the input queue
		for latest_video in latest_videos_to_make:
			input_queue.put(latest_video)
		
		# Make the latest videos in parralel threads
		run_threads(target=thread_make_latest_videos, kwargs={'input_queue': input_queue, 'video_frame_rate': video_frame_rate})
	
	else:
		logging.debug('No latest video to make')


def thread_make_latest_videos(input_queue, video_frame_rate = 24, video_size = None, video_bitrate = None, video_title = None):
	
	while not input_queue.empty() and not stop_daemon.is_set():
		
		try:
			wavelength = input_queue.get_nowait()
		except Queue.Empty:
			continue
		
		# Start date of the latest video (depends on wavelength)
		date = round_to_hour(datetime.utcnow()) - timedelta(hours = latest_video_length[wavelength])
		
		# We make the list of video pieces
		video_pieces = list()
		for hours in range(latest_video_length[wavelength] + 1):
			video_piece = video_piece_pattern.format(date = date + timedelta(hours = hours), wavelength = wavelength)
			if os.path.exists(video_piece):
				video_pieces.append(video_piece)
			else:
				logging.warning('Video piece %s not found, skipping!', video_piece)
		
		if not video_pieces:
			logging.warning('No video pieces found to make latest video for wavelength %d, skipping!', wavelength)
			continue
		
		if video_title is None:
			video_title = 'Video of the last {hours} hours of AIA {wavelength}Å'.format(wavelength = wavelength, hours=latest_video_length[wavelength])
		
		# Make the video to a temp path as not to overwritte the latest video
		video_path = latest_video_pattern.format(wavelength=wavelength, suffix='mp4')
		temp_video_path = latest_video_pattern.format(wavelength=wavelength, suffix='tmp.mp4')
		make_directory(os.path.dirname(temp_video_path))
		make_directory(os.path.dirname(video_path))
		
		# We make the video
		if video_to_mp4_video(video_pieces, temp_video_path, video_frame_rate, video_title, video_size, video_bitrate):
			# Move the temp file to it's latest path
			logging.debug('Moving file %s to %s', temp_video_path, video_path)
			shutil.move(temp_video_path, video_path)
		else:
			logging.error('Error while making latest video for wavelength %d', wavelength)


def make_daily_videos(daily_videos_to_make):
	
	# Start date of daily videos
	date = round_to_hour(datetime.utcnow()) - timedelta(hours = time_span)
	
	# Add the missing daily videos
	for wavelength in AIA_wavelengths:
		for hours in range(time_span / 12):
			for video_date in get_daily_video_dates(date + timedelta(hours = 12 * hours)):
				video_path = daily_video_pattern.format(date = video_date, wavelength = wavelength, suffix='mp4')
				if not os.path.exists(video_path):
					logging.info('Video piece %s is missing, will be made', video_path)
					daily_videos_to_make.add((wavelength, video_date))
	
	if daily_videos_to_make:
		
		input_queue = Queue.Queue()
		
		# Add the daily videos to the input queue
		for daily_video in daily_videos_to_make:
			input_queue.put(daily_video)
		
		# Make the daily videos in parralel threads
		run_threads(target=thread_make_daily_videos, kwargs={'input_queue': input_queue, 'video_frame_rate': video_frame_rate})
	else:
		logging.debug('No daily video to make')

def thread_make_daily_videos(input_queue, video_frame_rate = 24, video_size = None, video_bitrate = None, video_title = None):
	
	while not input_queue.empty() and not stop_daemon.is_set():
		
		try:
			wavelength, date = input_queue.get_nowait()
		except Queue.Empty:
			continue
		
		# We make the list of video pieces
		video_pieces = list()
		for hours in range(24):
			video_piece = video_piece_pattern.format(date = date + timedelta(hours = hours), wavelength = wavelength)
			if os.path.exists(video_piece):
				video_pieces.append(video_piece)
			else:
				logging.warning('Video piece %s not found, skipping!', video_piece)
		
		if not video_pieces:
			logging.warning('No video pieces found to make daily video for date %s and wavelength %d, skipping!', date, wavelength)
			continue
		
		if video_title is None:
			video_title = 'Video of AIA {wavelength}Å from {start} to {end}'.format(wavelength = wavelength, start=date.isoformat(), end=(date+timedelta(hours=24)).isoformat())
		
		video_path = daily_video_pattern.format(date=date, wavelength=wavelength, suffix='mp4')
		make_directory(os.path.dirname(video_path))
		
		# We make the video
		if video_to_mp4_video(video_pieces, video_path, video_frame_rate, video_title, video_size, video_bitrate):
			# TODO should we make a temp video
			pass
		else:
			logging.error('Error while making daily video for date %s and wavelength %d', date, wavelength)


if __name__ == '__main__':
	
	# You need to force this environment variable, otherwise all child process will be forced to the same CPU
	os.environ['OPENBLAS_MAIN_FREE'] = '1'
	
	# Default name for the log file
	log_filename = os.path.splitext(sys.argv[0])[0] + '.log'
	
	# Get the arguments
	parser = argparse.ArgumentParser(description='Make AIA latest images and videos')
	parser.add_argument('--debug', '-d', default=False, action='store_true', help='Set the logging level to debug')
	parser.add_argument('--verbose', '-v', default=False, action='store_true', help='Set the logging level to info')
	parser.add_argument('--log_filename', '-l', default=log_filename, help='Overwrite the image if it already exists')
	parser.add_argument('--time_span', '-t', default=time_span, type=int, help='Duration in hours to go back in time for the creation of images and videos')
	parser.add_argument('--max_threads', '-m', default=max_threads, type=int, help='Max number of concurrent threads')

	# Parse the arguments
	args = parser.parse_args()
	
	if args.debug:
		log_level = logging.DEBUG
	elif args.verbose:
		log_level = logging.INFO
	else:
		log_level = logging.ERROR
	
	time_span = args.time_span
	
	max_threads = args.max_threads
	
	# Setup the logging
	logging.basicConfig(level = log_level, filename = args.log_filename, format='%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	
	logging.info('Starting deamon')
	
	# The stop_daemon will tell all threads to terminate gracefully
	stop_daemon = threading.Event()
	
	# We setup the termination signal
	signal.signal(signal.SIGINT, terminate_gracefully)
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	signal.signal(signal.SIGQUIT, terminate_gracefully)
	signal.signal(signal.SIGTERM, terminate_gracefully)
	
	# All the media to make
	latest_images_to_make = dict()
	video_pieces_to_make = set()
	latest_videos_to_make = set()
	daily_videos_to_make = set()
	
	# Last run times of functions
	last_run_times = dict.fromkeys(max_run_frequency.keys(), datetime.min)
	
	# List of bad fitsfiles not to process
	bad_fitsfiles = SharedCache()
	
	while not stop_daemon.is_set():
		
		# Make the images from fits files
		if last_run_times['make_images'] + max_run_frequency['make_images'] <= datetime.now():
			last_run_times['make_images'] = datetime.now()
			images = make_images()
		else:
			logging.debug('Not yet time to run make_images: waiting until %s', last_run_times['make_images'] + max_run_frequency['make_images'])
		
		# Process the images
		for image in images:
			# Add the corresponding video piece to be made
			video_pieces_to_make.add((image['wavelength'], round_to_hour(image['date'])))
			
			# If the image is older than the latest, add the latest image to be made
			if image['wavelength'] not in latest_images_to_make or image['date'] > latest_images_to_make[image['wavelength']]['date']:
				latest_images_to_make[image['wavelength']] = image
		
		# Make the latest images
		if last_run_times['make_latest_images'] + max_run_frequency['make_latest_images'] <= datetime.now():
			last_run_times['make_latest_images'] = datetime.now()
			make_latest_images(latest_images_to_make.values())
		else:
			logging.debug('Not yet time to run make_latest_images: waiting until %s', last_run_times['make_latest_images'] + max_run_frequency['make_latest_images'])
		
		# Make the video pieces
		if last_run_times['make_video_pieces'] + max_run_frequency['make_video_pieces'] <= datetime.now():
			last_run_times['make_video_pieces'] = datetime.now()
			video_pieces = make_video_pieces(video_pieces_to_make)
			video_pieces_to_make = set()
		else:
			logging.debug('Not yet time to run make_video_pieces: waiting until %s', last_run_times['make_video_pieces'] + max_run_frequency['make_video_pieces'])
		
		# Process the video pieces
		for video_piece in video_pieces:
			# Add the corresponding daily videos to be made
			for video_date in get_daily_video_dates(video_piece['date']):
				daily_videos_to_make.add((video_piece['wavelength'], video_date))
			
			# Add the corresponding latest video to be made
			if video_piece['date'] >= datetime.utcnow() - timedelta(hours = latest_video_length[video_piece['wavelength']]):
				latest_videos_to_make.add(video_piece['wavelength'])
		
		# Make the latest videos
		if last_run_times['make_latest_videos'] + max_run_frequency['make_latest_videos'] <= datetime.now():
			last_run_times['make_latest_videos'] = datetime.now()
			make_latest_videos(latest_videos_to_make)
			latest_videos_to_make = set()
		else:
			logging.debug('Not yet time to run make_latest_videos: waiting until %s', last_run_times['make_latest_videos'] + max_run_frequency['make_latest_videos'])
		
		# Make the daily videos
		if last_run_times['make_daily_videos'] + max_run_frequency['make_daily_videos'] <= datetime.now():
			last_run_times['make_daily_videos'] = datetime.now()
			make_daily_videos(daily_videos_to_make)
			daily_videos_to_make = set()
		else:
			logging.debug('Not yet time to run make_daily_videos: waiting until %s', last_run_times['make_daily_videos'] + max_run_frequency['make_daily_videos'])
		
		# Clean the bad_fitsfiles cache
		bad_fitsfiles.clean(timedelta(hours=time_span))
		
		# Compute the time of the daemon next run
		next_run_time = min(time + max_run_frequency[name] for name, time in last_run_times.items())
		logging.debug('Next deamon loop at %s', next_run_time)
		
		# If it is not yet time for the next run, we sleep a little
		while datetime.now() < next_run_time and not stop_daemon.is_set():
			sleep(1)
		
