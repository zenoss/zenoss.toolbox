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
import time
import re

'''
This script mounts an added scsi (host,id) to a target mount directory
This script must run as root, the volume is mounted as 777
'''


def parse_arguments():
    parser = argparse.ArgumentParser(description="4.x migration - mount/unmount scsi to/from a target directory")
    outputGroup = parser.add_mutually_exclusive_group()
    outputGroup.add_argument('-m', '--mount',
        dest='doMount',
        help='mount the given SCSI node to the mount point', action='store_true', default=False)
    outputGroup.add_argument('-u', '--umount',
        dest='doUnmount',
        help='Unmount the given SCSI node from the mount point', action='store_true', default=False)
    parser.add_argument('scsi', help='SCSI_host:SCSI_id', default='')
    parser.add_argument('volume', help='The target mount directory', default='')
    return parser.parse_args()


def get_sda_hostid():
    # extract the hostid from the existing sda disk
    _line = subprocess.check_output(['/bin/ls', '-l', '/sys/block/sda'])
    _m = re.search('.*/host([0-9]+)/.*', _line)
    if _m:
        return _m.group(1)
    else:
        return ''


def scsi_umount(args, scsi_host, scsi_id):
    if args.scsi:
        # continue execute the statements even if failed
        rc1 = subprocess.call("umount %s" % args.volume, shell=True)
        rc2 = subprocess.call('echo "1" > /sys/class/scsi_host/host%s/device/target%s:0:%s/%s:0:%s:0/delete' %
            (scsi_host, scsi_host, scsi_id, scsi_host, scsi_id), shell=True)
        if rc1 == 0 and rc2 == 0:
            print '%s unmounted OK.' % args.volume
        else:
            print '%s unmount attempted ...' % args.volume


def scsi_mount(args, scsi_host, scsi_id):
    # partition, format, and mount the directory
    # if failed, exit

    try:
        # rescan to make sure that all unscanned device appears
        print "Identifying the new SCSI disk at %s:%s ..." % (scsi_host, scsi_id)
        subprocess.check_call('echo "- - -" > /sys/class/scsi_host/host%s/scan' % scsi_host, shell=True)
        time.sleep(1)

        # delete to identify the target one, if previously added
        # print "Delete host%s:%s first..." % (scsi_host, scsi_id)
        subprocess.check_call('echo "1" > /sys/class/scsi_host/host%s/device/target%s:0:%s/%s:0:%s:0/delete' %
                             (scsi_host, scsi_host, scsi_id, scsi_host, scsi_id), shell=True)
        time.sleep(1)
        old_dl = subprocess.check_output(
            "lsblk -o TYPE,KNAME,SIZE | awk '{if (index(\"disk\", $1)>0) {printf \"%s [%s]\\n\", $2, $3}}'", shell=True).split('\n')

        # rescan to find the dev ID
        subprocess.check_call('echo "- - -" > /sys/class/scsi_host/host%s/scan' % scsi_host, shell=True)
        time.sleep(1)
        new_dl = subprocess.check_output(
            "lsblk -o TYPE,KNAME,SIZE | awk '{if (index(\"disk\", $1)>0) {printf \"%s [%s]\\n\", $2, $3}}'", shell=True).split('\n')

        # print "New disks list:", new_dl

        delta_dl = list(set(new_dl) - set(old_dl))
        if len(delta_dl) != 1:
            raise Exception
        else:
            newdev = delta_dl[0]

        print "New device identified -> /dev/%s" % newdev

        try:
            raw_input("WARNING: Erasing (target%s:0:%s) /dev/%s !!! <ENTER> to continue, <CTRL+C> to quit ..." % (scsi_host, scsi_id, newdev))
            raw_input("WARNING: Press <ENTER> again to confirm ...")
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
        subprocess.call('echo "1" > /sys/class/scsi_host/host%s/device/target%s:0:%s/%s:0:%s:0/delete' %
            (scsi_host, scsi_host, scsi_id, scsi_host, scsi_id), shell=True)
        print 'Operation cancelled!'
        raise

    except:
        print 'Invalid SCSI node (%s)!' % args.scsi
        raise

    return


def main():
    try:
        args = parse_arguments()
        __scsi_host, scsi_id = args.scsi.split(':')

        # we require the scsi be added in the same host location as sda (disk1)
        # usually, it is 0
        scsi_host =  get_sda_hostid()

        if __scsi_host != scsi_host:
            print "Using /dev/sda scsi location %s: instead of %s:" % (scsi_host, __scsi_host)

        if not scsi_host or not scsi_id:
            raise Exception

        if not (args.doMount or args.doUnmount):
            args.doMount = True

        if args.doMount:
            scsi_mount(args, scsi_host, scsi_id)

        if args.doUnmount:
            scsi_umount(args, scsi_host, scsi_id)

    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print str(e)
        sys.exit(1)

if __name__ == '__main__':
    main()
