import uuid
import subprocess
import struct
import zlib
import lzma
import time
import datetime
import hashlib
import os
import os.path
from threading import Thread

def p_warning(msg):
	print(msg)

def p_alert(msg):
	print(msg)

def p_normal(msg):
	print(msg)

def p_debug(msg):
	print(msg)

def p_utility(msg):
	print(msg)

BLOCK_TYPE_UNKNOWN = 0
BLOCK_TYPE_NTFSCLONE = 1
BLOCK_TYPE_UNKNOWNPART = 2

BACKUP_DEVICE = '/dev/sda'

def get_uid_for_system():
	p = subprocess.Popen(['dmidecode'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	lines = p.communicate()[0].decode('utf8').split('\n')

	for line in lines:
		line = line.strip()

		parts = line.split(' ')

		if len(parts) > 1 and parts[0].find('UUID') == 0:
			return 'DMI' + parts[1]

	return 'PY' + uuid.getnode()

class Part:
	def __init__(self, dev, pdev, start, end, count):
		self.dev = dev
		self.pdev = pdev
		self.start = start
		self.end = end
		self.count = count

		# Check if NTFS system.
		self.is_ntfs = self.is_ntfs_check()

	def is_ntfs_check(self):
		p = subprocess.Popen(
			['ntfsinfo', '-m', self.pdev],
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
		)

		lines = p.communicate()[0].decode('utf8').split('\n')

		for line in lines:
			if line.find('Free Clusters') > -1:
				return True

		return False		

	def serialize_ntfs_to(self, fd):
		if not self.is_ntfs:
			return False 

		fd.write(struct.pack('B', BLOCK_TYPE_NTFSCLONE))
		szpos = fd.tell()
		fd.write(struct.pack('<Q', 0))
		fd.write(struct.pack('<Q', 0))

		p = subprocess.Popen(
			['ntfsclone', self.pdev, '-s', '-o', '-'], 
			stdout=subprocess.PIPE
		)

		comp = zlib.compressobj()

		bytes_wrote = 0

		while True:
			chunk = p.stdout.read(1024 * 1024 * 16)
			if len(chunk) == 0:
				break
			chunk = comp.compress(chunk)
			fd.write(chunk)
			bytes_wrote += len(chunk)

		p_debug('WROTE NTFSCLONE PARTITION', bytes_wrote)
		
		chunk = comp.flush()
		bytes_wrote += len(chunk)
		fd.write(chunk)

		tmp = fd.tell()
		fd.seek(szpos)
		fd.write(struct.pack('<Q', bytes_wrote))
		fd.seek(tmp)

		return True

	def serialize_to(self, fd):
		p_debug('WORKING ON PARTITION', self.pdev, self.start)

		if self.serialize_ntfs_to(fd) is True:
			return True

		xfd = open(self.pdev, 'rb')

		fd.write(struct.pack('B', BLOCK_TYPE_UNKNOWNPART))
		szpos = fd.tell()
		fd.write(struct.pack('<Q', 0))
		fd.write(struct.pack('<Q', 0))

		comp = zlib.compressobj()

		bytes_wrote = 0

		while True:
			chunk = xfd.read(1024 * 1024 * 16)
			if len(chunk) == 0:
				break
			chunk = comp.compress(chunk)
			fd.write(chunk)
			bytes_wrote += len(chunk)

		chunk = comp.flush()
		fd.write(chunk)
		bytes_wrote += len(chunk)

		p_debug('WROTE UNKNOWN PARTITION', bytes_wrote)

		tmp = fd.tell()
		fd.seek(szpos)
		fd.write(struct.pack('<Q', bytes_wrote))
		fd.seek(tmp)

		return True

def get_part_info(dev):
	p = subprocess.Popen(
		['fdisk', '-l', dev], 
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
	)

	lines = p.communicate()[0].decode('utf8').split('\n')

	out = {}

	unitsz = 512

	for line in lines:
		line = line.strip()

		if len(line) < 1:
			continue

		if line.find('Units') == 0:
			unitsz = int(line.split('=')[1].strip().split(' ')[0])
			continue

		if line[0] != '/':
			continue

		tmp = line.split(' ')
		parts = []

		for tmpart in tmp:
			if tmpart != '':
				parts.append(tmpart)

		pdev = parts[0]
		
		if parts[1] == '*':
			i = 2
		else:
			i = 1

		start = int(parts[i+0]) * unitsz
		end = int(parts[i+1]) * unitsz + unitsz - 1
		count = int(parts[i+2]) * 512
		sizestr = parts[i+3]
		typestr = parts[i+4]

		out[pdev] = Part(dev, pdev, start, end, count)

	return out

def copyblock(dev, coffset, length, fd):
	p_debug('BLOCK COPY %s BYTES' % length)

	fd.write(struct.pack('B', BLOCK_TYPE_UNKNOWN))
	szpos = fd.tell()
	fd.write(struct.pack('<Q', length))	
	fd.write(struct.pack('<Q', coffset))
	
	xfd = open(dev, 'rb')
	xfd.seek(coffset)

	chunksz = 1024 * 1024 * 16

	bytes_read = 0

	comp = zlib.compressobj()

	st = time.time()

	bytes_wrote = 0

	while length > 0:
		if chunksz > length:
			chunksz = length
		chunk = xfd.read(chunksz)
		bytes_read += len(chunk)
		length -= len(chunk)
		if len(chunk) < 1:
			raise Exception('End of device abruptly reached.')

		chunk = comp.compress(chunk)
		fd.write(chunk)
		bytes_wrote += len(chunk)

		if time.time() - st > 5:
			st = time.time()
			p_debug('BLOCK COPY %.1f%% COMPLETE' % (bytes_read / length))

	chunk = comp.flush()
	fd.write(chunk)
	bytes_wrote += len(chunk)

	tmp = fd.tell()
	fd.seek(szpos)
	fd.write(struct.pack('<Q', bytes_wrote))
	fd.seek(tmp)

	xfd.close()

	p_debug('BLOCK COPY COMPLETE')

"""
	Represents a smart cloned block device.

	On creation, the device will be intelligently
	cloned to minimize space usage and maximum
	throughput to bandwidth limited storage devices.
"""
class DeviceSmartClone:
	def __init__(self, dev, outfile):
		self.dev = dev
		self.outfile = outfile

		fd = open(outfile, 'wb')

		fd.write(struct.pack('<Q', int(time.time())))

		parts = get_part_info(BACKUP_DEVICE)

		coffset = 0

		for pdev in parts:
			p_debug('DETECTED', pdev)

		while True:
			near_delta = None
			near_part = None

			for pdev in parts:
				part = parts[pdev]
				delta = part.start - coffset
				if delta > -1 and (near_delta is None or delta < near_delta):
					near_delta = delta
					near_part = part 
					p_debug('SELECTED NEAR PART', pdev)

			if near_part is not None:
				# Handle this space as a non-partition block.
				unknown_size = near_part.start - coffset
				print('WORKING ON UNKNOWN', unknown_size, coffset)

				copyblock(dev, coffset, unknown_size, fd)

				# This will properly serialize the partition. It 
				# will also write a header with a type and length 
				# field.
				near_part.serialize_to(fd)
				# Jump to the last byte of the partition and repeat
				# the process.
				p_debug('near_part.end=%s' % near_part.end)
				coffset = near_part.end + 1
			else:
				xfd = open(dev, 'rb')
				xfd.seek(0, 2)
				devsz = xfd.tell()
				xfd.close()
	
				p_debug('COFFSET=%s' % coffset)		
				
				copyblock(dev, coffset, devsz - coffset, fd)
				break
		fd.close()

def get_uid_for_partitions():
	# return the blkid and the uuid.getnode()
	p = subprocess.Popen(['blkid'], stdout=subprocess.PIPE)

	lines = p.communicate()[0].decode('utf8').split('\n')

	out = {}

	for line in lines:
		line = line.strip()
		
		pos = line.find(':')
			
		part = line[0:pos].strip()
		label = line[pos+1:].strip()

		m = hashlib.md5()
		m.update(label.encode('utf8'))

		out[part] = m.hexdigest()

	return out

def get_valid_backup_path():
	return '/home/kmcguire/backupsys/temp'

	parts = get_uid_for_partitions()

	backup_path = None

	for part in parts:
		if os.path.exists('./%s/' % part) is False:
			os.makedirs('./%s/' % part)
		p = subprocess.Popen(['mount', part, './%s/' % part], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		lines = p.communicate()[0].decode('utf8').split('\n')

		# Check for the backup denotation.
		if os.path.exists('./%s/backup.drive' % part) is True:
			backup_path = './%s/' % part

		p = subprocess.Popen(['umount', './%s/' % part], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		lines = p.communicate()[0].decode('utf8').split('\n')

		if backup_path is not None:
			break

	return backup_path

def ThreadWriter(fdin, fdout):
	chunksz = 1024 * 1024 * 16
	while True:
		chunk = fdin.read(chunksz)
		fdout.write(chunk)

class Block:
	def __init__(self, source, btype, boffset, offset, bsize):
		self.btype = btype 		# block type in backup image
		self.boffset = boffset  # offset into target device
		self.offset = offset    # offset into source file
		self.bsize = bsize 		# size of (compressed) data
		self.source = source    # source of backup image

	def write_ntfsclone_to(self, dev):
		p = subprocess.Popen(
			['ntfsclone', '-', '-r', '--overwrite', dev],
			stdin=subprocess.PIPE
		)

		p_debug('NTFSCLONE writing to %s' % dev)

		#writer = Thread(target = ThreadWriter, args = (p.stdout, fd))
		#writer.start()

		# Writes the blocks out of order.
		#fd.seek(self.boffset)

		xfd = open(self.source, 'rb')
		xfd.seek(self.offset)

		chunksz = 1024 * 1024 * 16
		left = self.bsize

		comp = zlib.decompressobj()

		while left > 0:
			if chunksz > left:
				chunksz = left
			chunk = xfd.read(chunksz)
			left -= len(chunk)
			chunk = comp.decompress(chunk)
			if len(chunk) > 0:
				p.stdin.write(chunk)

		chunk = comp.flush()
		if len(chunk) > 0:
			p.stdin.write(chunk)

		p_debug('WAITING ON NTFSCLONE TO FINISH')
		p.wait()
		p_debug('NTFSCLONE FINISHED')

		#print('WAITING ON WRITER THREAD')
		#writer.join()
		#print('WRITER THREAD JOINED')

	def write_to(self, dev):
		if self.btype == BLOCK_TYPE_NTFSCLONE:
			return self.write_ntfsclone_to(dev)

		fd = open(dev, 'wb')

		# Allows writing the blocks out of order.
		fd.seek(self.boffset)

		p_debug('writing block %s %s %s %s' % (self.btype, self.offset, self.bsize, self.source))
		xfd = open(self.source, 'rb')
		xfd.seek(self.offset)

		chunksz = 1024 * 1024 * 16
		left = self.bsize

		comp = zlib.decompressobj()

		while left > 0:
			if chunksz > left:
				chunksz = left
			chunk = xfd.read(chunksz)
			left -= len(chunk)
			chunk = comp.decompress(chunk)
			if len(chunk) > 0:
				fd.write(chunk)

		chunk = comp.flush()
		if len(chunk) > 0:
			fd.write(chunk)

		fd.close()

class Backup:
	""" Write the backup onto the destination file descriptor.

	This restores the backup by writting the data represented
	to the file descriptor provided. At this point, all blocks
	should have been verified as valid to some extent and of
	course enumerated. This will prevent a partial and failed
	recovery if possible.
	"""
	def write_to(self, dev):
		# First write all the non-partition blocks since
		# the idea is to get the disk structure back in
		# place. This allows special utilities like ntfsclone
		# to operate on partitions. The ntfsclone can efficiently
		# restore the partition in this way.
		p_debug('WRITING NON-PARTITIONS %s' % dev)
		for block in self.blocks:
			if block.btype == BLOCK_TYPE_UNKNOWN:
				# These blocks have an absolute offset
				# into the whole block device.
				block.write_to(dev)

		devname = dev[dev.rfind('/')+1:]

		time.sleep(3)

		# Tell the kernel to rescan the disk.
		if devname:
			p_debug('SYNCING PARTITIONS TO KERNEL')
			xfd = open('/sys/block/%s/device/rescan' % devname, 'w')
			xfd.write('1')
			xfd.close()

		time.sleep(3)

		p_debug('WRITING PARTITIONS')

		pnum = 1

		for block in self.blocks:
			if block.btype != BLOCK_TYPE_UNKNOWN:
				# These blocks are relative to the actual
				# partition.
				block.write_to(dev + str(pnum))
				pnum += 1

	def get_desc(self):
		return self.desc

	def get_date_string(self):
		return self.datestr

	def __init__(self, node, fullpath):
		self.fullpath = fullpath
		self.node = node
		self.parts = node.split('_')
		self.valid = False
		self.this_machine = False
		self.blocks = []

		if len(self.parts) < 3:
			return

		if self.parts[0] != 'backup':
			return

		self.valid = True

		muid = get_uid_for_system()

		if self.parts[1] == muid:
			self.this_machine = True

		self.desc = self.parts[2]

		fd = open(fullpath, 'rb')

		self.unixtime = struct.unpack('<Q', fd.read(8))[0]
		self.datestr = datetime.datetime.fromtimestamp(self.unixtime).strftime('%m-%d-%Y-%H:%M')

		coffset = 8

		# Enumerate all the blocks and do any verification
		# if it is needed.
		while True:
			hdr = fd.read(1 + 8 * 2)

			# Expected
			if len(hdr) == 0:
				break

			# Not Expected
			if len(hdr) != 1 + 8 * 2:
				self.valid = False
				return

			(btype, bsz, boffset) = struct.unpack('<BQQ', hdr)
			#print('btype=%s bsz=%s' % (btype, bsz))

			if btype > BLOCK_TYPE_UNKNOWNPART:
				self.valid = False
				return

			self.blocks.append(Block(fullpath, btype, boffset, fd.tell(), bsz))

			coffset += (bsz & 0xffffffff) + 1 + 8 + 8

			fd.seek(coffset)

		if len(self.blocks) < 1:
			self.valid = False

class Backups:
	def __init__(self):
		# Get a valid backup path.
		self.bupath = get_valid_backup_path()

		if self.bupath is None:
			self.connected = False
			return

		self.connected = True

		# Scan the backups.
		nodes = os.listdir(self.bupath)

		self.bunodes = []

		for node in nodes:
			bu = Backup(node, '%s/%s' % (self.bupath, node))
			if bu.valid and bu.this_machine:
				self.bunodes.append(bu)

	def get_backup_path(self):
		return self.bupath

	def is_storage_connected(self):
		return self.connected

	def get_machine_backups(self):
		return self.bunodes

def is_valid_desc(desc):
	for c in desc:
		if c.isalnum() or c == ' ' or c == '-':
			continue
		return False
	return True

def do_backup(bus):
	desc = None
	while desc is None:
		p_normal('TYPE A DESCRIPTIVE NAME FOR THIS BACKUP')
		p_normal('=======================================')
		p_normal('MUST BE LESS THAN 25 CHARACTERS')
		p_normal('MUST USE ONLY LETTERS, NUMBERS, SPACES, AND HYPHENS')
		p_normal('=======================================')
		desc = input('TYPE NAME:')
		if is_valid_desc(desc) is False:
			desc = None
			p_normal('=======================================')
			p_normal('INVALID DESCRIPTIVE NAME. TRY AGAIN.')

	# backup_<mid>_<desc>_<unixtime>

	bupath = bus.get_backup_path()
	muid = get_uid_for_system()
	cunixtime = int(time.time())

	fpath = '%s/backup_%s_%s_%s' % (bupath, muid, desc, cunixtime)

	p_normal('=======================================')
	p_normal('STARTING BACKUP')
	p_normal('=======================================')
	DeviceSmartClone(BACKUP_DEVICE, fpath)

def do_restore(bus):
	backups = bus.get_machine_backups()

	backups = sorted(backups, key=lambda bu: bu.unixtime, reverse=True)

	cndx = 0
	choice = None

	while True:
		p_normal('')
		sndx = cndx
		p_normal('NUM | DESCRIPTION')
		for x in range(0, 10):
			p_normal('%s   | %s %s' % (cndx, backups[cndx].get_date_string(), backups[cndx].get_desc()))
			cndx = (cndx + 1) % len(backups)
			if cndx == sndx:
				break
		p_normal('=======================================')
		p_normal('TYPE THE NUMBER SPECIFIED TO RESTORE THAT BACKUP')
		p_normal('PRESS ONLY ENTER TO SEE NEXT 10 CHOICES')
		p_normal('TYPE "exit" TO EXIT TO MAIN MENU')
		p_normal('=======================================')
		choice = input('NUMBER:')

		if choice == '':
			continue

		if choice == 'exit':
			return False

		try:
			choice = int(choice)
		except:
			print('THE CHOICE "%s" WAS NOT UNDERSTOOD' % choice)
			return False

		break

	# USER HAS MADE THEIR CHOICE
	cbu = backups[choice]
	p_normal('=======================================')
	p_normal('ARE YOU SURE YOU WISH TO RESTORE THE BACKUP %s %s?' % (cbu.get_date_string(), cbu.get_desc()))
	p_normal('=======================================')
	choice = input('TYPE yes TO CONTINUE:')

	if choice != 'yes':
		return False

	p_normal('=======================================')
	p_normal('RESTORING BACKUP')
	cbu.write_to(BACKUP_DEVICE)
	p_normal('RESTORATION DONE')
	p_normal('=======================================')
	return True

def main():
	bus = Backups()

	if bus.is_storage_connected() is False:
		p_normal('STORAGE NOT CONNECTED')
		input('PRESS ANY KEY TO SEARCH AGAIN')
		return

	if len(bus.get_machine_backups()) > 0:
		p_alert('PREVIOUS BACKUPS FOUND FOR THIS MACHINE')
		p_normal('')
		p_normal('TYPE "restore" TO SELECT A BACKUP TO RESTORE')

	p_normal('TYPE "backup" TO CREATE A BACKUP')
	choice = input('')

	if choice == 'restore':
		p_normal('=======================================')
		p_normal('====  RESTORING DISK                 ==')
		p_normal('=======================================')
		if do_restore(bus) is True:
			p_normal('=======================================')
			p_normal('====  RESTORE COMPLETE               ==')
			p_normal('=======================================')
			while True:
				input()
	elif choice == 'backup':
		p_normal('=======================================')
		p_normal('====  BACKING UP DISK                ==')
		p_normal('=======================================')
		do_backup(bus)
		p_normal('=======================================')
		p_normal('====  BACKUP COMPLETE                ==')
		p_normal('=======================================')
		while True:
			input()
	else:
		p_warning('THE CHOICE "%s" WAS NOT UNDERSTOOD')
		return


	#DeviceSmartClone(BACKUP_DEVICE, './tmp/test')
	#SmartCloneDeploy('./tmp/test', BACKUP_DEVICE)

while True:
	try:
		main()
	except IOError as e:
		p_warning('(A) THE STORAGE DEVICE MAY BE FULL')
		p_warning('(B) THE TARGET DISK MAY BE BAD')
		input('PRESS ANY KEY TO CONTINUE')
