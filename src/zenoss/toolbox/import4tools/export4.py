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
import datetime
import os
import statvfs
import subprocess
import sys
import tempfile
import shutil
import re

from validate4import import ValidationRunner
from validate4import import parse_argz as parseVRunnerArgs

import Globals
from Products.ZenUtils.Utils import unused
unused(Globals)
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.ZenUtils.GlobalConfig import globalConfToDict


# grouping configuration and global variables
class GL:
    tmp_dir =               None
    backup_dir =            os.path.join(os.environ['ZENHOME'], 'backups')
    flexera_dir =           os.path.join(os.environ['ZENHOME'], 'var', 'flexera')
    ucsx_vers =             ['1.1.0', '1.1.1']

    dmd_uuid_filename =     'dmd_uuid.txt'
    components_filename =   'componentList.txt'
    md5_filename =          'backup.md5'

    dmd = 0
    args = 0
    backupSize = 0
    thetime = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    target_vol = None   # used only by scsi mount target dir
    target_dir =  ''    # to be initialized in init()
    target_file = ''    # to be initialized in init()
    target_path = ''    # to be intiialized in init()

    scsi_host = ''
    scsi_id = ''
    sda_host = '0'

    # we initialize the derived variables here
    @classmethod
    def init(cls):
        # determine the absolute path to target_dir
        # if scsi is given, we use /mnt/export4/4x-backup
        if cls.args.scsi:
            cls.target_vol = os.path.join(os.path.sep, 'mnt', 'export4')
            cls.target_dir = os.path.join(cls.target_vol, '4x-backup')
            cls.target_file = '4x-export-%s.tar' % cls.thetime
        # if filename is given, we use the absolute path to the filename
        elif cls.args.filename:
            cls.target_file = os.path.basename(cls.args.filename)
            # add .tar extension if needed
            fbase, fext = os.path.splitext(cls.target_file)
            if fext != '.tar':
                cls.target_file = cls.target_file + '.tar'
            cls.target_dir = os.path.dirname(os.path.abspath(cls.args.filename))
        # else, we use the current dir
        else:
            cls.target_file = '4x-export-%s.tar' % cls.thetime
            cls.target_dir = os.path.abspath('.')

        cls.target_path = os.path.join(cls.target_dir, cls.target_file)

    @classmethod
    def init_tmp(cls):
        # honor the temp dir specified
        if cls.args.temp_dir:
            cls.tmp_dir = tempfile.mkdtemp(dir=cls.args.temp_dir)
        else:
            cls.tmp_dir = tempfile.mkdtemp(dir=cls.target_dir)

        # now create the tmp dir
        cls.dmd_uuid_filename =     os.path.join(cls.tmp_dir, 'dmd_uuid.txt')
        cls.components_filename =   os.path.join(cls.tmp_dir, 'componentList.txt')
        cls.md5_filename =          os.path.join(cls.tmp_dir, 'backup.md5')


def parse_arguments():
    parser = argparse.ArgumentParser(description="4.x export script")
    outputGroup = parser.add_mutually_exclusive_group()
    outputGroup.add_argument('-f', '--filename', help='the name of export file. export is created in the current directory. if unspecified, name is 4x-export-YYmmdd-HHMMSS.tar', default=None)
    outputGroup.add_argument('-s', '--scsi', help='the linux device id of a device on the same scsi_host as /dev/sda.', default=None)
    outputGroup.add_argument('--dry-run', help='perform a dry run of the backup, and report the estimated required disk space for the backup', action='store_true')
    parser.add_argument('-z', '--no-zodb', help="don't backup zodb.", action='store_true', default=False)
    parser.add_argument('-e', '--no-eventsdb', help="don't backup events.", action='store_true', default=False)
    parser.add_argument('-p', '--no-perfdata', help="don't backup perf data (won't backup remote collectors unnecessarily).", action='store_true', default=False)
    parser.add_argument('-d', '--debug', help="debug mode", action='store_true', default=False)
    parser.add_argument('--temp-dir', help='temporary directory for intermediate files', dest='temp_dir', action='store', default=None)
    GL.args = parser.parse_args()


def get_collector_list():
    if not hasattr(GL.dmd.Monitors, 'Hub'):
        print 'Not using distributed collectors.'
        return []
    colldict = {}
    for hub in GL.dmd.Monitors.Hub.objectSubValues():
        for collector in hub.collectors():
            if collector.isLocalHost():
                continue
            if collector.hostname in colldict:
                print 'collector %s shares a hostname with collector %s, skipping duplicate.' % (collector.id, colldict[collector.hostname].id)
                continue
            colldict[collector.hostname] = collector
    if not colldict:
        print 'All collectors are local.'
        return []
    collectors = []
    for coll in colldict.itervalues():
        if '' in (coll.hub().id, coll.id, coll.hostname):
            print 'skipping %s because information is missing (one of [hubID/collectorID/collectorHostname]): %s' % (coll.id, [coll.hub().id, coll.id, coll.hostname])
            continue
        collectors.append(','.join([collector.hub().id, collector.id, collector.hostname]))
    return collectors


def backup_remote_collectors(backup_dir):
    if GL.args.no_perfdata:
        return []
    remote_backups = []
    sys.stderr.write('Getting remote collector information.\n')
    for line in get_collector_list():
        hub, collector, hostname = line.split(',')
        remote_backup_filename = '%s-%s-perf-backup-%s.tgz' % (hub, collector, GL.thetime)
        remote_backup_fn = os.path.join(backup_dir, remote_backup_filename)
        remotebackupcmd = ['dc-admin', '--hub-pattern', hub, '--collector-pattern', collector, 'exec', '/opt/zenoss/bin/zenbackup', '--file=%s' % remote_backup_fn, '--no-eventsdb', '--no-zodb']
        remotezbresult = subprocess.call(remotebackupcmd)
        if remotezbresult is not 0:
            print 'backup failed on remote collector %s, aborting ...' % collector
            cleanup(False)
            sys.exit(remotezbresult)
        scpcmd = ['scp', 'zenoss@%s:%s' % (hostname, remote_backup_fn), GL.tmp_dir]
        scpresult = subprocess.call(scpcmd)
        if scpresult is not 0:
            print 'failed to scp backup %s from remote collector %s, aborting ...' % (remote_backup_filename, collector)
            cleanup(False)
            sys.exit(scpresult)
        remote_backups.append(remote_backup_filename)
    return remote_backups


def backup_master(backup_dir):
    print 'making new backup ...'
    before_dir = set(os.listdir(backup_dir))
    zbcommand = ['zenbackup']
    if GL.args.temp_dir:
        zbcommand.append('--temp-dir=%s' % GL.tmp_dir)
    if GL.args.no_zodb:
        zbcommand.append('--no-zodb')
    if GL.args.no_eventsdb:
        zbcommand.append('--no-eventsdb')
    if GL.args.no_perfdata:
        zbcommand.append('--no-perfdata')

    try:
        raw_input("All Zenoss services will be stopped to export data for migration.\n"
                "The services are restarted once the backup completes.\n\n"
                "Press ENTER to continue or <CTRL+C> to quit\n")
    except KeyboardInterrupt:
        raise

    subprocess.check_call(['zenoss', 'stop'])
    try:
        zbresult = subprocess.call(zbcommand)
        if zbresult is not 0:
            print 'no backup specified and making one failed, aborting ...'
            raise Exception
    except:
        cleanup(False)
        sys.exit(1)
    finally:
        subprocess.call(['zenoss', 'start'])

    after_dir = set(os.listdir(backup_dir))
    backup_path = os.path.join(backup_dir, list(after_dir - before_dir)[0])
    return backup_path


def export_component_list():
    print 'exporting component list ...'
    devcount = 0
    with open(GL.components_filename, 'w') as fp:
        for dev in GL.dmd.Devices.getSubDevices():
            fp.write('### components for %s' % '/'.join(dev.getPrimaryPath()) + '\n')
            for comp in dev.getMonitoredComponents():
                fp.write('/'.join(comp.getPrimaryPath()) + '\n')
            devcount += 1
            if devcount % 100 is 0:
                print 'exported 100 devices'
    print 'a total of %d devices in export' % devcount
    print 'component list exported'


def export_dmduuid():
    with open(GL.dmd_uuid_filename, 'w') as fp:
        fp.write(GL.dmd.uuid + '\n')
    print 'dmd uuid exported'


def genmd5(master_backup_path):
    _cmd = 'md5sum -b %s > %s' % (master_backup_path, GL.md5_filename)
    _rc = subprocess.call(_cmd, shell=True)
    if _rc != 0:
        print 'Generating md5 failed'
        cleanup(False)
        sys.exit(_rc)


def add_to_tar(tar_name, path_name):
    _pn = os.path.split(path_name)
    _tcmd = 'tar -C %s -rf %s %s' % (_pn[0], tar_name, _pn[1])
    _tcmd_rc = subprocess.call(_tcmd, shell=True)
    if _tcmd_rc is not 0:
        print 'Adding %s to %s failed!' % (path_name, tar_name)
        cleanup(False)
        sys.exit(_tcmd_rc)


def make_export_tar(tar_file, components_filename, remote_backups, master_backup_path, flexera_dir):
    add_to_tar(tar_file, components_filename)
    add_to_tar(tar_file, GL.dmd_uuid_filename)

    for _one in remote_backups:
        add_to_tar(tar_file, "%s/%s" % (GL.tmp_dir, _one))

    if os.path.isdir(flexera_dir):
        add_to_tar(tar_file, flexera_dir)

    add_to_tar(tar_file, master_backup_path)
    add_to_tar(tar_file, GL.md5_filename)

    print 'export successful. file is %s' % tar_file


def cleanup(error=False):
    if GL.args:
        if not GL.args.debug:
            try:
                shutil.rmtree(GL.tmp_dir)
                if GL.args.scsi:
                    print "Unmounting - enter root password when prompted ->"
                    subprocess.check_call(["/bin/su", "-c" "/opt/zenoss/bin/use_scsi -u %s:%s %s" % (GL.sda_host, GL.args.scsi, GL.target_vol)])
            except:
                pass
        if GL.target_path and error:
            try:
                os.remove(GL.target_path)
            except:
                pass


def dryRun():
    """
    Report back the estimated disk space needed for the backup.  Zenoss can be running for this.
    """
    backupSize = 0      # estimated size of backup in MB (GL.backupsize is in GB)
    # skip remote collectors (TODO)
    backupSize += 5     # md5, dmd_uuid, flexera, componentList
    backupSize += 10    # bin, etc, backup.settings

    # Global Catalog (if it exists)
    if os.path.exists('/opt/zenoss/var/zencatalogservice'):
        catalogDataSize = subprocess.check_output("du -sc /opt/zenoss/var/zencatalogservice | awk 'END{print $1;}'", shell=True)
        catalogDataSize = int(int(catalogDataSize.strip()) / 1000) + 1
        backupSize += catalogDataSize
        print 'Local catalog data estimated to need %d MB' % catalogDataSize

    # ZenPacks
    zenpackDataSize = subprocess.check_output("du -sc /opt/zenoss/ZenPacks | awk 'END{print $1;}'", shell=True)
    zenpackDataSize = int(int(zenpackDataSize.strip()) / 1000) + 1
    backupSize += zenpackDataSize
    print 'Local zenpack data estimated to need %d MB' % zenpackDataSize

    # DB estimate (does not include routines, but it's very fast).  Grabs table
    # sizes from information_schema for the given DB
    def getDBSize(db, dbName=None):
        if db not in ('zodb', 'zep'):
            print "ERROR: Bad database string: %s" % db
            sys.exit(1)
        globalSettings = globalConfToDict()
        user = globalSettings.get('%s-admin-user' % db, None)
        if not user:
            print 'ERROR: Unable to determine admin db user for %s' % db
            sys.exit(1)
        cmd = ['mysql', '-s', '--skip-column-names', '-u%s' % str(user)]
        host = globalSettings.get('%s-host' % db, None)
        if host and host != 'localhost':
            cmd.append('-h%s' % str(host))
        port = globalSettings.get('%s-port' % db, None)
        if port and str(port) != '3306':
            cmd.append('--port=%s' % str(port))
        cred = globalSettings.get('%s-admin-password' % db, None)
        if cred:
            cmd.append('-p%s' % str(cred))
        if not dbName:
            dbName = globalSettings.get('%s-db' % db, None)     # Save this for later
        if not dbName:
            print 'ERROR: Unable to locate database name in global config'
            sys.exit(1)

        cmd.append('-e')
        selectStr = "\"SELECT Data_BB / POWER(1024,2) FROM (SELECT SUM(data_length) Data_BB FROM information_schema.tables WHERE table_schema = '%s') A;\"" % dbName
        cmd.append(selectStr)

        DBSize = int(float(subprocess.check_output(' '.join(cmd), shell=True).strip())) + 1
        return DBSize

    # ZEP db
    zepDBSize = int(getDBSize('zep') * .05) + 1
    backupSize += zepDBSize
    print 'Estimated zeneventserver database dump size is %d MB' % zepDBSize

    # ZODB db
    zodbDBSize = int(getDBSize('zodb') * .1) + 1
    backupSize += zodbDBSize
    print 'Estimated zodb database dump size is %d MB' % zodbDBSize

    # ZODB session db
    zodbSessionDBSize = int(getDBSize('zodb', dbName='zodb_session') * .05) + 1
    backupSize += zodbSessionDBSize
    print 'Estimated zodb session db dump size is %d MB' % zodbSessionDBSize

    # ZEP indexes
    zepIndexDataSize = subprocess.check_output("du -sc /opt/zenoss/var/zeneventserver/index | awk 'END{print $1;}'", shell=True)
    zepIndexDataSize = int(int(zepIndexDataSize.strip()) / 1000) + 1
    backupSize += zepIndexDataSize
    print 'Zeneventserver indexes estimated to need %d MB' % zepIndexDataSize

    # Local perf data
    perfDataSize = subprocess.check_output("du -sc /opt/zenoss/perf | awk 'END{print $1;}'", shell=True)
    perfDataSize = int(int(perfDataSize.strip()) / 1000) + 1
    backupSize += perfDataSize
    print 'Local performance data estimated to need %d MB' % perfDataSize

    # After staging everything individually, it gets tarred up, so at worst, it
    # needs double
    backupSize *= 2.0
    GL.backupSize = backupSize/1000

    # adding 10% buffer
    print 'Total estimated free space needed for export is up to %d GB' % (GL.backupSize * 1.1 + 1)


def freeSpaceG(fname):
    f = os.statvfs(fname)
    size = (f[statvfs.F_BSIZE] * f[statvfs.F_BAVAIL]) / (1000*1000*1000)
    return size


def checkSpace():
    dryRun()

    # check tmp dir
    avail = freeSpaceG(GL.tmp_dir)
    if avail < GL.backupSize:
        print "Insufficient temp space in %s: %d GB, %d GB is needed." % (GL.tmp_dir, avail, GL.backupSize)
        cleanup(False)
        sys.exit(1)
    else:
        print "Available temporary space of %s: %d GB." % (GL.tmp_dir, avail)

    # check target dir
    avail = freeSpaceG(GL.target_path)
    dname = os.path.dirname(GL.target_dir)
    if avail < GL.backupSize:
        print "Insufficient backup space in %s: %d GB, %d GB is needed." % (dname, avail, GL.backupSize)
        cleanup(False)
        sys.exit(1)
    else:
        print "Available backup space of %s: %d GB." % (dname, avail)


def get_sda_hostid():
    # extract the hostid from the existing sda disk
    _line = subprocess.check_output(['/bin/ls', '-l', '/sys/block/sda'])
    _m = re.search('.*/host([0-9]+)/.*', _line)
    if _m:
        return _m.group(1)
    else:
        return ''


def prep_scsi():
    # partition, format, and mount the directory
    # if failed, exit

    # mount the provided scsi disk
    try:
        GL.sda_host = get_sda_hostid()
        if not GL.sda_host:
            raise

        print "Preparing disk - enter root password when prompted ->"
        subprocess.check_call(["/bin/su", "-c", "/opt/zenoss/bin/use_scsi -m %s:%s %s" % (GL.sda_host, GL.args.scsi, GL.target_vol)])

    except:
        print 'Failed to prepare %s as the export4 disk!' % GL.args.scsi
        sys.exit(1)

    subprocess.check_call(['/bin/mkdir', '-p', GL.target_dir])
    subprocess.check_call(['/bin/chmod', '-v', '777', GL.target_dir])
    print "Export space prepared..."

    return


def prep_target():
    # preparing the target area based on GL.args.scsi and GL.args.filename
    # the result is stored in GL.target_path

    if GL.args.scsi:
        prep_scsi()

    # check accessibility of the tar file
    print 'Checking accessibility of %s ...' % GL.target_file
    try:
        if not os.path.exists(GL.target_dir):
            os.makedirs(GL.target_dir)
        if os.path.isfile(GL.target_path):
            os.remove(GL.target_path)
        with open(GL.target_path, 'w'):
            print '%s is accessible ...' % GL.target_file
    except:
        if GL.args.scsi:
            print 'Cannot access SCSI node (%s)! please check host/id ...' % GL.args.scsi
        else:
            print 'Cannot open %s! please check accessibility ...' % GL.target_path

        cleanup(False)
        sys.exit(1)

    return


def main():
    try:
        parse_arguments()

        # based on args, we initialize globally used variables
        GL.init()

        if GL.args.dry_run:
            print 'Calculating free space needed for backup'
            dryRun()
            sys.exit()

        print 'Running validations'
        validations = [ValidationRunner(parseVRunnerArgs(["zenpack"])).run()]
        if any(validations):
            print 'Validation(s) failed, aborting export process'
            sys.exit(1)

        # exit if target preparation failed
        prep_target()

        # now all target dirs are ready, we generate the tmp_dir and files
        GL.init_tmp()

        if not os.path.isdir(GL.backup_dir):
            os.makedirs(GL.backup_dir)

        GL.dmd = ZenScriptBase(noopts=True, connect=True).dmd

        print 'Checking platform ...'
        try:
            ucsx = None
            ucsx = GL.dmd.ZenPackManager.packs._getOb('ZenPacks.zenoss.UCSXSkin')
        except:
            pass

        if ucsx:
            if ucsx.version in GL.ucsx_vers:
                print 'UCSPM version %s is supported.' % ucsx.version
            else:
                print 'UCSPM version %s is not supported ...' % ucsx.version
                sys.exit(1)

        # always do a dryRun to make sure the tmp and target dirs have
        # sufficient space
        checkSpace()

        remote_backups = backup_remote_collectors(GL.backup_dir)
        master_backup = backup_master(GL.backup_dir)

        export_component_list()
        export_dmduuid()
        genmd5(master_backup)
        make_export_tar(GL.target_path, GL.components_filename, remote_backups, master_backup, GL.flexera_dir)

    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print str(e)
        cleanup(error=True)
        sys.exit(1)

    finally:
        cleanup(error=False)

if __name__ == '__main__':
    main()
