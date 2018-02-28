#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
import subprocess
import tempfile
import logging

def run_command(command):
	return run_command_with_input_data(command, input_data = None)


def run_command_with_input_data(command, input_data = None):
	
	try:
		logging.debug("About to execute %s", ' '.join(command))
		process = subprocess.Popen(command, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds = True)
		stdout, stderr = process.communicate(input=input_data)
		return_code = process.wait()
		if return_code != 0:
			logging.error('Failed running command %s :\n Return code : %d\n StdOut: %s\n StdErr: %s', ' '.join(command), return_code, stdout, stderr)
			return False
		elif logging.root.isEnabledFor(logging.DEBUG):
			logging.debug('Succeful running command %s :\n Return code : %d\n StdOut: %s\n StdErr: %s', ' '.join(command), return_code, stdout, stderr)
		return True
	except Exception, why:
		logging.error('Failed running command %s : %s', ' '.join(command), why)
		return False


def run_command_with_input_files(command, input_filenames = []):
	
	# We redirect the stdout and stderr to temporary files
	try:
		stdout_file = tempfile.TemporaryFile()
		stderr_file = tempfile.TemporaryFile()
	except Exception, why:
		logging.critical('Failed creating temporary files for stout and stderr : %s', why)
		return False
	
	try:
		logging.debug("About to execute %s", ' '.join(command))
		process = subprocess.Popen(command, bufsize = 1024 * 1024, shell=False, stdin=subprocess.PIPE, stdout=stdout_file, stderr=stderr_file, close_fds = True)
		
		# We write the content of the input files to the stdin of the process
		for input_filename in input_filenames:
			try:
				with open(input_filename, 'rb') as input_file:
					logging.debug("Writing input file %s to process stdin", input_filename)
					process.stdin.write(input_file.read())
			except Exception, why:
				logging.error("Error writing input file %s to process stdin: %s", input_filename, why)
		
		process.stdin.close()
		
		# We wait for the process to terminate
		logging.debug("Waiting to terminate process for: %s", ' '.join(command))
		return_code = process.wait()
		
		# Read the stdout and stderr content
		stdout_file.seek(0)
		stdout = stdout_file.read()
		stdout_file.close()
		stderr_file.seek(0)
		stderr = stderr_file.read()
		stderr_file.close()
		
		if return_code != 0:
			logging.error('Failed running command %s :\n Return code : %d\n StdOut: %s\n StdErr: %s', ' '.join(command), return_code, stdout, stderr)
			return False
		elif logging.root.isEnabledFor(logging.DEBUG):
			logging.debug('Succeful running command %s :\n Return code : %d\n StdOut: %s\n StdErr: %s', ' '.join(command), return_code, stdout, stderr)
		
		return True
	except Exception, why:
		logging.error('Failed running command %s : %s', ' '.join(command), why)
		return False
