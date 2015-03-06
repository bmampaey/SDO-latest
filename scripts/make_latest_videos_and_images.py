#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
import threading
import Queue
import sys, os, os.path, glob
import subprocess
import pyfits
import shutil
import signal
import time
from datetime import datetime, timedelta
import dateutil.parser
import logging
from make_video import *

# Script to generate images and videos from aia quicklook fits files for the sdodata latest website 

# Max number of cpu and threads
max_cpu = 6
max_threads = 10

# Run frequency in seconds
run_frequency = 180

#Acceptable quality bits (See SDO keywords) 
min_quality = (1 << 8) + (1 << 9) + (1 << 13) + (1 << 30)

# Directoriy of the fits files
fitsfiles_directory = '/data/SDO/public/AIA_quicklook'

# Fits files wavelengths
wavelengths = [94, 131, 171, 193, 211, 304, 335, 1600, 1700, 4500]

# Directories for the images
images_directory = '/data/SDO/latest/images'
latest_images_directory = '/data/SDO/latest/images/latest'

# Directories for the videos
videos_directory = '/data/SDO/latest/videos'
videos_pieces_directory = '/data/SDO/latest/videos_pieces'
latest_videos_directory = '/data/SDO/latest/videos/latest'

# Parameters to transform fits to png
fits2png_bin = '/home/sdo/SPoCA/bin/fits2png.x'
large_size = '1024x1024>' 

# Parameters for ImageMagick
convert_bin = 'convert'
medium_size = '128x128>'
small_size = '45x45>'

# parameters for videos
video_frame_rate = 16

# Duration in hours to go back in time for the creation of images and videos
max_time_span = 48

# Duration in hours of the latest videos 
latest_video_length = 24



def setup_logging(filename = None, quiet = False, verbose = False, debug = False):
	global logging
	if debug:
		logging.basicConfig(level = logging.DEBUG, format='%(levelname)-8s: %(message)s')
	elif verbose:
		logging.basicConfig(level = logging.INFO, format='%(levelname)-8s: %(message)s')
	else:
		logging.basicConfig(level = logging.CRITICAL, format='%(levelname)-8s: %(message)s')
	
	if quiet:
		logging.root.handlers[0].setLevel(logging.CRITICAL + 10)
	elif verbose:
		logging.root.handlers[0].setLevel(logging.INFO)
	else:
		logging.root.handlers[0].setLevel(logging.CRITICAL)
	
	if filename:
		import logging.handlers
		fh = logging.handlers.TimedRotatingFileHandler(filename, 'midnight', 1)
		fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
		if debug:
			fh.setLevel(logging.DEBUG)
		else:
			fh.setLevel(logging.INFO)
		
		logging.root.addHandler(fh)

directory_creation_lock = threading.Lock()

def make_directory(directory):
	'''Create a directory and all the subdirectories'''
	if not os.path.isdir(directory):
		basedir, trash = os.path.split(directory)
		make_directory(basedir)
		with directory_creation_lock:
			if not os.path.isdir(directory):
				os.mkdir(directory)

def get_fitsfiles(directory, max_time_span):
	
	fitsfiles = list()
	now = datetime.utcnow()
	for t in range(max_time_span , -1, -1):
		directory_date = now - timedelta(hours = t)
		directory_path = os.path.join(directory, directory_date.strftime('%Y/%m/%d/H%H00'))
		logging.debug("Getting fits files for directory %s", directory_path)
		fitsfiles.extend(sorted(glob.glob(os.path.join(directory_path, '*.fits'))))
	
	return fitsfiles


def get_keywords(fitsfile, keywords):
	result = [None]*len(keywords)
	try:
		hdulist = pyfits.open(fitsfile)
		for k,keyword in enumerate(keywords):
			for hdu in hdulist:
				if keyword in hdu.header:
					result[k] = hdu.header[keyword]
					break
		
		hdulist.close()
	except IOError, why:
		logging.critical("Error reading keywords from file %s: %s", fitsfile, str(why))
	return result

def fits2image(fitsfile, image_directory, size):
	
	# We run fits2png to make the image
	fits2png = [fits2png_bin, fitsfile, '-u', '-R', '512.5,512.5', '-L', '-c', '-S', size, '-O', image_directory]
	logging.debug("About to execute: %s", ' '.join(fits2png))
	try:
		process = subprocess.Popen(fits2png, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout, stderr = process.communicate()
		return_code = process.poll()
		if return_code != 0:
			logging.error('Failed running command %s :\nReturn code : %d\n StdOut: %s\n StdErr: %s', ' '.join(fits2png), return_code, stdout, stderr)
			return False
	except Exception, why:
		logging.critical('Failed running command %s : %s', ' '.join(fits2png), str(why))
		return False
	else:
		return True

def make_thumbnail(image_file, thumbnail_file, size):
	
	convert = [convert_bin, image_file, '-resize', size, thumbnail_file]
	logging.debug("About to execute: %s", ' '.join(convert))
	try:
		process = subprocess.Popen(convert, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout, stderr = process.communicate()
		return_code = process.poll()
		if return_code != 0:
			logging.error('Failed running command %s :\nReturn code : %d\n StdOut: %s\n StdErr: %s', ' '.join(convert), return_code, stdout, stderr)
			return False
	except Exception, why:
		logging.critical('Failed running command %s : %s', ' '.join(convert), str(why))
		return False
	else:
		return True


def make_button(image_file, button_file, size):
	
	convert = [convert_bin, image_file, '-resize', size, '-fuzz', '10%', '-transparent', 'black', button_file]
	logging.debug("About to execute: %s", ' '.join(convert))
	try:
		process = subprocess.Popen(convert, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout, stderr = process.communicate()
		return_code = process.poll()
		if return_code != 0:
			logging.error('Failed running command %s :\nReturn code : %d\n StdOut: %s\n StdErr: %s', ' '.join(convert), return_code, stdout, stderr)
			return False
	except Exception, why:
		logging.critical('Failed running command %s : %s', ' '.join(convert), str(why))
		return False
	else:
		return True


def thread_make_image(fitsfile, images_queue, images_base_directory, cpu_lock):
	
	# We get the necessary keywords
	date_obs, wavelength, quality = get_keywords(fitsfile, ['DATE-OBS', 'WAVELNTH', 'QUALITY'])
	
	# We check the date
	try:
		date_obs = dateutil.parser.parse(date_obs)
	except Exception, why:
		logging.info("DATE-OBS keyword in file %s (%s) is invalid: %s. Skipping!", fitsfile, date_obs, str(why))
		return
	
	# We check if the file already exists
	image_directory = os.path.join(images_base_directory, date_obs.strftime('%Y/%m/%d/H%H00'))
	image_path = os.path.join(image_directory, os.path.splitext(os.path.basename(fitsfile))[0]+ '.png')
	if os.path.isfile(image_path):
		logging.debug('Fits file %s already converted to image %s. Skipping!', fitsfile, image_path)
		return
	
	# We check the wavelength
	try:
		wavelength = int(wavelength)
	except Exception, why:
		logging.info("WAVELNTH keyword in file %s (%s) is invalid. Skipping!", fitsfile, wavelength)
		return
	else:
		if wavelength not in wavelengths:
			logging.warning('Unknown wavelength %s for file %s. Skipping!', wavelength, fitsfile)
			return
	
	# We check the quality
	try:
		quality = int(quality)
	except Exception, why:
		logging.info("QUALITY keyword in file %s (%s) is invalid. Skipping!", fitsfile, quality)
		return
	else:
		if quality | min_quality != min_quality:
			logging.info("Quality of file %s (%s) does not meet the minimum required quality, skipping!", fitsfile, quality)
			return
	
	# We make the image directory
	try:
		make_directory(image_directory)
	except Exception, why:
		logging.critical("Cannot create directory %s: %s", image_directory, str(why))
		return
	
	
	# We make the image
	with cpu_lock:
		if terminate_thread.is_set():
			return
		logging.info("Making image for file %s", fitsfile)
		if fits2image(fitsfile, image_directory, large_size):
			images_queue.put({'date':date_obs, 'wavelength': wavelength, 'path': image_path, 'directory': image_directory})
		
		else:
			logging.info('File %s not converted to image. Skipping!', fitsfile)


def thread_make_video_piece_from_images(images_base_directory, videos_pieces_base_directory, date, wavelength, cpu_lock, video_frame_rate = 24):
	
	# We make the list of frames
	images_directory = os.path.join(images_base_directory, date.strftime('%Y/%m/%d/H%H00'))
	images = sorted(glob.glob(os.path.join(images_directory, '*%04d.png' % wavelength)))
	
	if len(images) < 1:
		logging.error("No enough images found to make video piece for date %s and wavelength %d", str(date), wavelength)
		return
	
	# We make the videos pieces directory
	videos_pieces_directory = os.path.join(videos_pieces_base_directory, date.strftime('%Y/%m/%d/H%H00'))
	try:
		make_directory(videos_pieces_directory)
	except Exception, why:
		logging.critical("Cannot create directory %s: %s", videos_pieces_directory, str(why))
		return
	
	# We make the video piece
	video_path = os.path.join(videos_pieces_directory, date.strftime('AIA.%Y%m%d_%H0000.') + ('%04d.ts' % wavelength))
	with cpu_lock:
		if terminate_thread.is_set():
			return
		png_to_ts_video(images, video_path, frame_rate = video_frame_rate, video_title = None, video_size = None, video_bitrate = None, video_preset='slow')


def thread_make_latest_video_from_videos_pieces(videos_pieces_base_directory, latest_videos_directory, date, wavelength, cpu_lock, video_frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We make the list of video pieces
	videos_pieces = list()
	for hours in range(latest_video_length):
		videos_pieces_directory = os.path.join(videos_pieces_base_directory, (date + timedelta(hours = hours)).strftime('%Y/%m/%d/H%H00'))
		video_piece = sorted(glob.glob(os.path.join(videos_pieces_directory, '*%04d.ts' % wavelength)))
		if len(video_piece) >= 1:
			videos_pieces.append(video_piece[0])
			if len(video_piece) > 1:
				logging.warning("Found more than one video piece for wavelength %d in directory %s", wavelength, videos_pieces_directory)
		else:
			logging.warning("Found no video piece for wavelength %d in directory %s", wavelength, videos_pieces_directory)
	
	if not videos_pieces:
		logging.error("No video pieces found to make video for date %s and wavelength %d", str(date), wavelength)
		return
	
	# We make the latest videos directory
	try:
		make_directory(latest_videos_directory)
	except Exception, why:
		logging.critical("Cannot create directory %s: %s", latest_videos_directory, str(why))
		return
	
	# We make the video
	video_path = os.path.join(latest_videos_directory, 'AIA.latest.%04d' % wavelength)
	with cpu_lock:
		if terminate_thread.is_set():
			return
		video_to_mp4_video(videos_pieces, video_path+'.mp4', video_frame_rate, video_title, video_size, video_bitrate)
	
	with cpu_lock:
		if terminate_thread.is_set():
			return
		video_to_webm_video(videos_pieces, video_path+'.webm', video_frame_rate, video_title, video_size, video_bitrate)
	
	with cpu_lock:
		if terminate_thread.is_set():
			return
		video_to_ogv_video(videos_pieces, video_path+'.ogv', video_frame_rate, video_title, video_size, video_bitrate)


def thread_make_video_from_videos_pieces(videos_pieces_base_directory, videos_base_directory, date, wavelength, cpu_lock, video_frame_rate = 24, video_title = None, video_size = None, video_bitrate = None):
	
	# We make the list of video pieces
	videos_pieces = list()
	for hours in range(24):
		videos_pieces_directory = os.path.join(videos_pieces_base_directory, (date + timedelta(hours = hours)).strftime('%Y/%m/%d/H%H00'))
		video_piece = sorted(glob.glob(os.path.join(videos_pieces_directory, '*%04d.ts' % wavelength)))
		if len(video_piece) >= 1:
			videos_pieces.append(video_piece[0])
			if len(video_piece) > 1:
				logging.warning("Found more than one video piece for wavelength %d in directory %s", wavelength, videos_pieces_directory)
		else:
			logging.warning("Found no video piece for wavelength %d in directory %s", wavelength, videos_pieces_directory)
	
	if not videos_pieces:
		logging.error("No video pieces found to make video for date %s and wavelength %d", str(date), wavelength)
		return
	
	# We make the latest videos directory
	videos_directory = os.path.join(videos_base_directory, date.strftime('%Y/%m/%d/'))
	try:
		make_directory(videos_directory)
	except Exception, why:
		logging.critical("Cannot create directory %s: %s", videos_directory, str(why))
		return
	
	# We make the video
	video_path = os.path.join(videos_directory, date.strftime('AIA.%Y%m%d_%H0000.') + ('%04d.mp4' % wavelength))
	with cpu_lock:
		if terminate_thread.is_set():
			return
		video_to_mp4_video(videos_pieces, video_path, video_frame_rate, video_title, video_size, video_bitrate)


def terminate_gracefully(signal, frame):
	logging.info("Received signal %s: Exiting gracefully", str(signal))
	terminate_thread.set()
	sys.exit(0)

def start_thread(target, args=(), kwargs={}, name='Unknown', group=None):
	while threading.active_count() > max(1, max_threads):
		logging.debug("Too many threads are active, waiting")
		time.sleep(1)
	
	logging.debug("Starting thread %s", name)
	thread = threading.Thread(group=group, name=name, target=target, args=args, kwargs=kwargs)
	thread.daemon = True
	thread.start()

if __name__ == "__main__":
	
	script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
	# Default name for the log file
	log_filename = os.path.join('/home/sdo/latest/', script_name+'.log')

	# Setup the logging
	setup_logging(filename = log_filename, quiet = False, verbose = False, debug = False)
	
	log = logging.getLogger(script_name)
	
	log.info("Starting deamon")

	# We setup the termination signal
	signal.signal(signal.SIGINT, terminate_gracefully)
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	signal.signal(signal.SIGQUIT, terminate_gracefully)
	signal.signal(signal.SIGTERM, terminate_gracefully)
	
	# The terminate_thread will tell threads to terminate gracefully
	terminate_thread = threading.Event()
	
	# We increase the max_threads by the number of threads already running
	max_threads += threading.active_count()
	
	# We make a queue for the images
	images_queue = Queue.Queue()
	
	# The dates of the latest images, so we don't redo unnecessary work
	latest_image_dates = dict.fromkeys(wavelengths, None)
	
	cpu_lock = threading.Semaphore(max_cpu)
	
	start_time = time.time()
	
	while True:
		
		# The videos pieces missing or for which new frames have been created will have to be redone
		videos_pieces_to_remake = dict()
		now = datetime.utcnow()
		now = datetime(year = now.year, month = now.month, day = now.day, hour = now.hour)
		for wavelength in wavelengths:
			videos_pieces_to_remake[wavelength] = set()
			for hours in range(max_time_span):
				date = now - timedelta(hours = hours)
				video_piece_path = os.path.join(videos_pieces_directory, date.strftime('%Y/%m/%d/H%H00'), date.strftime('AIA.%Y%m%d_%H0000.') + ('%04d.ts' % wavelength))
				if not os.path.exists(video_piece_path):
					logging.info("Video piece %s is missing, will be remade", video_piece_path)
					videos_pieces_to_remake[wavelength].add(date)
		
		# We get the fitsfiles
		fitsfiles = get_fitsfiles(fitsfiles_directory, max_time_span)
		
		# We transorm the fitsfiles to images
		while fitsfiles:
			fitsfile = fitsfiles.pop()
			start_thread(target=thread_make_image, args=(fitsfile, images_queue, images_directory, cpu_lock), name='thread_make_image')
		
		# We process the images
		while not images_queue.empty() or threading.active_count() > 1:
			
			try:
				image = images_queue.get(True, 1)
			except (Queue.Empty), nomore:
				continue
			
			# Because of the new image we need to remake the corresponding video
			videos_pieces_to_remake[image['wavelength']].add(datetime(year = image['date'].year, month = image['date'].month, day = image['date'].day, hour = image['date'].hour))
			
			# We copy the latest frame as the latest image, and make thumbnails 
			if not latest_image_dates[image['wavelength']] or image['date'] > latest_image_dates[image['wavelength']]:
				
				latest_image_dates[image['wavelength']] = image['date']
				
				latest_image_basename = 'AIA.latest.%04d' % image['wavelength']
				latest_image_path = os.path.join(latest_images_directory, latest_image_basename + ".large.png")
				
				log.debug('Copying %s to %s', image['path'], latest_image_path)
				try:
					make_directory(latest_images_directory)
					shutil.copy(image['path'], latest_image_path)
				except Exception, why:
					log.critical('Error copying %s to %s: %s', image['path'], latest_image_path, str(why))
				
				# We use the large image to create the corresponding thumbnails
				if os.path.isfile(latest_image_path):
					small_thumbnail = os.path.join(latest_images_directory, latest_image_basename + ".small.png")
					make_thumbnail(latest_image_path, small_thumbnail, small_size)
					medium_thumbnail = os.path.join(latest_images_directory, latest_image_basename + ".medium.png")
					make_thumbnail(latest_image_path, medium_thumbnail, medium_size)
					button = os.path.join(latest_images_directory, latest_image_basename + ".button.png")
					make_button(medium_thumbnail, button, medium_size)
					
				else:
					log.info('Not creating thumbnails for image %s, it is missing!', latest_image_path)
		
		
		# The latest videos missing or for which new videos pieces have been created will have to be redone
		latest_videos_to_remake = set()
		for wavelength in wavelengths:
				latest_video_path = os.path.join(latest_videos_directory, 'AIA.latest.%04d.mp4' % wavelength)
				if not os.path.exists(latest_video_path):
					logging.info("Latest video %s is missing, will be remade", latest_video_path)
					latest_videos_to_remake.add(wavelength)
		
		now = datetime.utcnow()
		latest_date = datetime(year = now.year, month = now.month, day = now.day, hour = now.hour) - timedelta(hours = latest_video_length)
		
		# The videos for which new videos pieces have been created will have to be redone
		videos_to_remake = dict()
		for wavelength in wavelengths:
			videos_to_remake[wavelength] = set()
		
		# We remake the videos pieces
		for wavelength, dates in videos_pieces_to_remake.iteritems():
			for date in dates:
				start_thread(target=thread_make_video_piece_from_images, args=(images_directory, videos_pieces_directory, date, wavelength, cpu_lock, video_frame_rate), name='thread_make_video_piece_from_images')
				video_date = datetime(year = date.year, month = date.month, day = date.day, hour = int(date.hour/12)*12)
				videos_to_remake[wavelength].add(video_date)
				videos_to_remake[wavelength].add(video_date - timedelta(hours = 12))
				if date >= latest_date:
					latest_videos_to_remake.add(wavelength)
		
		# We wait that all threads have terminated
		while threading.active_count() > 1:
			log.debug("Waiting: %d videos to be finished", threading.active_count() - 1)
			time.sleep(1)
		
		# We remake the latest videos
		for wavelength in latest_videos_to_remake:
			start_thread(target=thread_make_latest_video_from_videos_pieces, args=(videos_pieces_directory, latest_videos_directory, latest_date, wavelength, cpu_lock, video_frame_rate, 'Latest video of AIA {wavelength}Å'.format(wavelength = wavelength), '720x720'), name='thread_make_video_from_videos_pieces')
		
		# We remake the videos
		for wavelength, dates in videos_to_remake.iteritems():
			for date in dates:
				start_thread(target=thread_make_video_from_videos_pieces, args=(videos_pieces_directory, videos_directory, date, wavelength, cpu_lock, video_frame_rate), name='thread_make_video_from_videos_pieces')
		
		# We wait that all threads have terminated
		while threading.active_count() > 1:
			log.debug("Waiting: %d videos to be finished", threading.active_count() - 1)
			time.sleep(1)
		
		# If it terminated faster than the run_frequency, we sleep a little
		while (time.time() - start_time < run_frequency) and (not terminate_thread.is_set()):
			log.debug("Going to sleep for %s more seconds.", start_time + run_frequency - time.time())
			time.sleep(1)
		
		start_time = time.time()
