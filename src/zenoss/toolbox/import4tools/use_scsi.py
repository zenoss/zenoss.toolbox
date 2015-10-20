#!/opt/zenoss/bin/python
##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

scriptVersion = "0.9"

import argparse
import subprocess
import sys
import os
import time

'''
This script mounts an added scsi (scsi_host,id) on a linux dev tree to a target mount directory
This script must run as root, the volume is mounted as 777
'''

def parse_arguments():
    parser = argparse.ArgumentParser(description="4.x migration - mount/unmount scsi to/from a target directory")
    parser.add_argument('-s', '--size', type=int, dest='theSize', default=0,
                        help='check if the device has at least the given size')
    outputGroup = parser.add_mutually_exclusive_group()
    outputGroup.add_argument('-m', '--mount',
        dest='doMount',
        help='mount the given SCSI node to the mount point', action='store_true', default=False)
    outputGroup.add_argument('-u', '--umount',
        dest='doUnmount',
        help='Unmount the given SCSI node from the mount point', action='store_true', default=False)
    parser.add_argument('volume', help='The target mount directory', default='')
    return parser.parse_args()


# to detect any newly added device
def scan_hosts():
    # rescan all scsi hosts (this is LINUX specific)
    for _host in os.listdir('/sys/class/scsi_host'):
        subprocess.check_call('echo "- - -" > %s/%s/scan' %
                              ('/sys/class/scsi_host', _host), shell=True)
    time.sleep(1)


# to detect if devices are removed
def rescan_devices():
    for _device in os.listdir('/sys/class/scsi_device'):
        subprocess.check_call('echo "1" > %s/%s/device/rescan' %
                              ('/sys/class/scsi_device', _device), shell=True)
    time.sleep(1)


def scsi_umount(args):
    # continue execute the statements even if failed
    subprocess.call("umount %s" % args.volume, shell=True)

    try:
        print ""
        print "Safe to remove the export disk from the virtual machine ..."
        raw_input("<ENTER> after removed ...")
        # recan and delete the device if removed
        rescan_devices()
    except KeyboardInterrupt:
        print "<Ctrl-C> User skipped the rescan of the export drive."


def scsi_mount(args):
    # partition, format, and mount the directory
    # if failed, exit

    try:
        # find the existing devices first
        scan_hosts()
        rescan_devices()
        old_dl = subprocess.check_output(
            "lsblk -o TYPE,KNAME | awk '{if (index(\"disk\", $1)>0) {printf \"%s\\n\", $2}}'", shell=True).split('\n')
        try:
            print ""
            print "Add the export disk to the virtual machine now."
            raw_input("<ENTER> when added, <CTRL+C> to quit ...")
        except KeyboardInterrupt:
            print "User quit the adding export disk process ..."
            raise Exception

        # rescan to find the new dev ID
        scan_hosts()
        rescan_devices()
        new_dl = subprocess.check_output(
            "lsblk -o TYPE,KNAME | awk '{if (index(\"disk\", $1)>0) {printf \"%s\\n\", $2}}'", shell=True).split('\n')

        # print "New disks list:", new_dl

        delta_dl = list(set(new_dl) - set(old_dl))
        if len(delta_dl) > 1:
            print "More than one device added ..."
            raise Exception
        elif len(delta_dl) == 0:
            print "No new device found ..."
            raise Exception
        else:
            newdev = delta_dl[0]

        print "New device identified -> /dev/%s" % newdev

        newdev = newdev.split()[0]
        # check if the space needs to be checked
        try:
            # use the GiB
            _GiB=1024*1024*1024
            _devSize = int(float(subprocess.check_output("blockdev --getsize64 /dev/%s" % newdev, shell=True).strip())/_GiB)
            if (args.theSize != 0) and (_devSize < args.theSize):
                raise Exception
        except:
            print "Expecting %d GiB ..." % args.theSize
            print "Device /dev/%s [%d GiB] is not sufficient ..." % (newdev, _devSize)
            raise Exception

        try:
            print ""
            print "WARNING: Ready to prepare /dev/%s [%d GiB] for export ..." % (newdev, _devSize)
            raw_input("<ENTER> to continue, <CTRL+C> to quit ...")
        except KeyboardInterrupt:
            raise

        newdev = newdev.split()[0]
        print "Preparing disk device /dev/%s ..." % newdev
        # clean all partitions
        subprocess.check_call("dd if=/dev/zero of=/dev/%s bs=512 count=1 conv=notrunc" % newdev, shell=True)

        # create a new primary partition spanning whole disk
        subprocess.check_call("echo -e 'n\np\n1\n\n\np\nw\nq\n' | fdisk /dev/%s" % newdev, shell=True)

        # make a ext4 file system on the new partition
        subprocess.check_call("mkfs -t ext4 -L IMPORT4 /dev/%s1" % newdev, shell=True)

        # create the target directory
        subprocess.check_call("mkdir -p %s" % args.volume, shell=True)

        # mount the new partition to the target directory
        subprocess.check_call("mount /dev/%s1 %s" % (newdev, args.volume), shell=True)

        # change to world permision of the target directory
        subprocess.check_call("chmod -v 777 %s" % args.volume, shell=True)

        print '%s mounted OK.' % args.volume

    except KeyboardInterrupt:
        rescan_devices()
        print 'Operation cancelled!'
        raise

    except:
        print 'Failed to mount SCSI drive!'
        raise

    return


def main():
    try:
        args = parse_arguments()

        if not (args.doMount or args.doUnmount):
            args.doMount = True

        if args.doMount:
            scsi_mount(args)

        if args.doUnmount:
            scsi_umount(args)

    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print str(e)
        sys.exit(1)

if __name__ == '__main__':
    main()
