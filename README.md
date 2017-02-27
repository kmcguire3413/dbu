# Disk Backup (DBU)
A utility that is intended to provide whole disk backup using minimal representation of the underlying device.

This minimal representation is accomplished by using built-in or utility programs such as `ntfsclone` part of the `ntfs-3g`
suite of tools. It is desired that other utilities or code can be added to support other file systems. This ability to have
specific support allows eliminating the copying of unused disk space which decreases the amount of time needed to make a 
whole disk backup. 

_This type of backup does not provide full disk forensics capabilities since some data such as unusued disk space is not copied. 
This unused disk space may contain important data that could be recovered using forensic methods._

Currently, only NTFS is supported. All other file system types and unused disk space is copied byte for byte.

GPT disks are not fully supported or tested but should work. Caution is advised on these disks.

Only the first block device is supported currently (/dev/sda); however, with minimal work would any disk could be selected.

# How it works

  * The utility `fdisk` is used to map out the partitions on the first block device
  * The partitions are probed for their file system type. Only NTFS is supported.
  * The disk is broken into blocks where blocks may be unknown, partition, or NTFS.
  * Each block is serialized into a file with any needed information.
  * On restoration, first the non-partitions are restored which restores the partition table. (MBR/GPT)
  * The kernel is told to rescan the disks.
  * Each partition is written back to disk using byte-for-byte or a special utility like `ntfsclone`.
