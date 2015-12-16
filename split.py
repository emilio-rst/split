#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  sin título.py
#  
#  Copyright 2012 emilio silveira <emilio.rst@gmail.com>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

from multiprocessing import Process, Lock, cpu_count
from multiprocessing.sharedctypes import RawValue
import ctypes
import os
import re
import math

class Status:
	""" Estado de proceso de división """
	
	def __init__(self, filesize):
		""" Constructor """
		
		# Tamaño de archivo
		self.filesize = filesize
		
		# Progreso
		self.progress = RawValue(ctypes.c_ulonglong, 0)
		
		# Bloqueo de progreso
		self.lock = Lock()
		
		# Procesos ejecutandose
		self.jobs = []
		
	def add_job(self, job):
		""" Adiciona un proceso """
		
		self.jobs.append(job)
		
	def terminate(self):
		""" Detiene el proceso """
		
		for job in self.jobs:
			job.terminate()
			
	def is_finished(self):
		""" Verifica si finalizó """
		
		for job in self.jobs:
			if job.is_alive():
				return False
		
		return True
		
	def add_progress(self, writed):
		""" Adiciona progreso en bytes """
		
		with self.lock:
			self.progress.value += writed
		
	def get_progress(self, percent = False):
		""" Obtiene el progreso del archivo"""
		
		with self.lock:
			progress = self.progress.value
			
		if percent:
			return int(round(100*float(progress)/self.filesize, 0))
			
		return progress
		

class Blocks:
	""" Grupo de bloques que se asociará a un proceso """
	
	# Cantidad de bytes a leer por vez
	size_read = 1048576 # 1M
	
	def __init__(self, filename, output, size, start, end, status):
		""" Constructor """
		
		self.filename = filename
		self.output = output
		self.size = size
		
		# Si el tamaño es incluso menor que el buffer designado
		if size < self.size_read:
			self.size_read = size
		
		self.start = start
		self.end = end
		self.status = status
		
	def write(self):
		""" Escribe los bloques en el disco"""
		
		with open(self.filename, 'rb') as f:
			f.seek((self.start - 1) * self.size)
			
			for i in range(self.start, self.end + 1):
				
				# Abrir el archivo de part para escribir
				with open('{0}.{1}'.format(self.output, i), 'wb') as part:
				
					read = self.size
					while read > 0:
						pos = f.tell()
						
						if read < self.size_read:
							# Para no proseguir y salir del bloque designado
							data = f.read(read)
							read = 0
						else:
							data = f.read(self.size_read)
							read -= self.size_read
				
						# Si es el fin del archivo
						if data == '':
							break
						
						part.write(data)
						self.status.add_progress(f.tell() - pos)
						
		
class Splitter:
	""" Divisor de archivo """
		
	def __init__(self, filename, size, threads = None):
		""" Constructor """
		
		self.filename = filename
		self.size = self.to_bytes(size)
		
		# Si no se indica la cantidad de hilos, se usa de manera predeterminada la cantidad de núcleos
		self.threads = cpu_count() if threads is None else threads
		
	def write(self, output = None):
		""" Escribe los bloques de archivo en el disco """
		
		if output is None:
			output = self.filename 
			
		# Obtiene el tamaño del archivo
		filesize = os.path.getsize(self.filename)
		
		# Número de bloques
		blocks_number = int(math.ceil(float(filesize)/self.size))
		
		# Avance de bloques por hilo
		v = int(math.floor(blocks_number/self.threads))
		
		# Objeto que refleja el estado del proceso de division
		status = Status(filesize)
		
		i = 1
		while i <= blocks_number:			
			# Si va a quedar sobrando un conjunto de bloques inferior al avance
			if (blocks_number - i - v) < v:
				blocks = Blocks(self.filename, output, self.size, i, blocks_number, status)
				i = blocks_number + 1
			else:
				blocks = Blocks(self.filename, output, self.size, i, i + v, status)
				i += v + 1
				
			job = Process(target=blocks.write)
			status.add_job(job)
				
			# Inicia el proceso
			job.start()
			
		return status
			
	@staticmethod
	def to_bytes(value):
		""" Convierte un valor de tamaño a bytes utilizando los sufijos B, K, M, G, T y P para Byte, Kilobyte, Megabyte, Gigabyte, Terabyte y Petabyte respectivamente """
		
		suffixes = {
			'B' : 1,
			'K' : 1024,
			'M' : 1024 ** 2,
			'G' : 1024 ** 3,
			'T' : 1024 ** 4,
			'P' : 1024 ** 5
		}

		m = re.match('^([0-9]+(\.[0-9]+)?)([BKMGTP]?)$', value)
		
		if m is None:
			raise Exception('No es una unidad de tamaño de archivo valida')
		
		suffix = m.group(3)
		
		if suffix is not None:
			return int(float(m.group(1)) * suffixes[suffix])
		else:
			return int(m.group(1))
		
		
def main():
	status = Splitter('prueba.zip', '1M').write()
	#status = Splitter('rld-sim3.iso', '1G').write()
	
	progress = 0
	progress_before = 0
	while not status.is_finished():
		progress = status.get_progress(True)
		if progress != progress_before:
			print progress,'%'
			progress_before = progress
			
	return 0

if __name__ == '__main__':
	main()

