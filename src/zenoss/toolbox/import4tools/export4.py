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

from validate4import import ValidationRunner
from validate4import import parse_argz as parseVRunnerArgs

import Globals
from Products.ZenUtils.Utils import unused
unused(Globals)
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.ZenUtils.GlobalConfig import globalConfToDict


class Config:
    tmp_dir =               None
    backup_dir =            os.path.join(os.environ['ZENHOME'], 'backups')
    flexera_dir =           os.path.join(os.environ['ZENHOME'], 'var', 'flexera')
    ucsx_vers =             ['1.1.0', '1.1.1']

    dmd_uuid_filename =     'dmd_uuid.txt'
    components_filename =   'componentList.txt'
    md5_filename =          'backup.md5'

    @classmethod
    def init(cls, tdir):
        if not tdir:
            tdir = '/tmp'

        cls.tmp_dir = tempfile.mkdtemp(dir=tdir)
        cls.dmd_uuid_filename =     os.path.join(cls.tmp_dir, 'dmd_uuid.txt')
        cls.components_filename =   os.path.join(cls.tmp_dir, 'componentList.txt')
        cls.md5_filename =          os.path.join(cls.tmp_dir, 'backup.md5')


class GL:
    dmd = 0
    args = 0
    backupSize = 0


def parse_arguments(thetime):
    parser = argparse.ArgumentParser(description="4.x export script")
    default_export_filename = '4x-export-%s.tar' % thetime
    dryRunGroup = parser.add_mutually_exclusive_group()
    dryRunGroup.add_argument('-f', '--filename', help='specify name of export file. export is created in the current directory. if unspecified, name is 4x-export-YYmmdd-HHMMSS.tar', default=default_export_filename)
    dryRunGroup.add_argument('--dry-run', help='perform a dry run of the backup, and report the estimated required disk space for the backup', action='store_true')
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


def backup_remote_collectors(thetime, backup_dir):
    if GL.args.no_perfdata:
        return []
    remote_backups = []
    sys.stderr.write('Getting remote collector information.\n')
    for line in get_collector_list():
        hub, collector, hostname = line.split(',')
        remote_backup_filename = '%s-%s-perf-backup-%s.tgz' % (hub, collector, thetime)
        remote_backup_fn = os.path.join(backup_dir, remote_backup_filename)
        remotebackupcmd = ['dc-admin', '--hub-pattern', hub, '--collector-pattern', collector, 'exec', '/opt/zenoss/bin/zenbackup', '--file=%s' % remote_backup_fn, '--no-eventsdb', '--no-zodb']
        remotezbresult = subprocess.call(remotebackupcmd)
        if remotezbresult is not 0:
            print 'backup failed on remote collector %s, aborting ...' % collector
            sys.exit(remotezbresult)
        scpcmd = ['scp', 'zenoss@%s:%s' % (hostname, remote_backup_fn), Config.tmp_dir]
        scpresult = subprocess.call(scpcmd)
        if scpresult is not 0:
            print 'failed to scp backup %s from remote collector %s, aborting ...' % (remote_backup_filename, collector)
            sys.exit(scpresult)
        remote_backups.append(remote_backup_filename)
    return remote_backups


def backup_master(backup_dir):
    print 'making new backup ...'
    before_dir = set(os.listdir(backup_dir))
    zbcommand = ['zenbackup']
    if GL.args.temp_dir:
        zbcommand.append('--temp-dir=%s' % GL.args.temp_dir)
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
        sys.exit(1)
    finally:
        subprocess.call(['zenoss', 'start'])

    after_dir = set(os.listdir(backup_dir))
    backup_path = os.path.join(backup_dir, list(after_dir - before_dir)[0])
    return backup_path


def export_component_list():
    print 'exporting component list ...'
    devcount = 0
    with open(Config.components_filename, 'w') as fp:
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
    with open(Config.dmd_uuid_filename, 'w') as fp:
        fp.write(GL.dmd.uuid + '\n')
    print 'dmd uuid exported'


def genmd5(master_backup_path):
    _cmd = 'md5sum -b %s > %s' % (master_backup_path, Config.md5_filename)
    _rc = subprocess.call(_cmd, shell=True)
    if _rc != 0:
        print 'Generating md5 failed'
        sys.exit(_rc)


def add_to_tar(tar_name, path_name):
    _pn = os.path.split(path_name)
    _tcmd = 'tar -C %s -rf %s %s' % (_pn[0], tar_name, _pn[1])
    _tcmd_rc = subprocess.call(_tcmd, shell=True)
    if _tcmd_rc is not 0:
        print 'Adding %s to %s failed!' % (path_name, tar_name)
        sys.exit(_tcmd_rc)


def make_export_tar(tar_file, components_filename, remote_backups, master_backup_path, flexera_dir):
    add_to_tar(tar_file, components_filename)
    add_to_tar(tar_file, Config.dmd_uuid_filename)

    for _one in remote_backups:
        add_to_tar(tar_file, "%s/%s" % (Config.tmp_dir, _one))

    if os.path.isdir(flexera_dir):
        add_to_tar(tar_file, flexera_dir)

    add_to_tar(tar_file, master_backup_path)
    add_to_tar(tar_file, Config.md5_filename)

    print 'export successful. file is %s' % tar_file


def cleanup(error=False):
    if GL.args:
        if not GL.args.debug:
            try:
                shutil.rmtree(Config.tmp_dir)
            except:
                pass
        if GL.args.filename and error:
            try:
                os.remove(GL.args.filename)
            except:
                pass


def dryRun():
    """
    Report back the estimated disk space needed for the backup.  Zenoss can be running for this.
    """
    backupSize = 0      # estimated size of backup in MB
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
    backupSize *= 2
    GL.backupSize = backupSize

    print 'Total estimated free space needed for export is up to %0d MB' % backupSize


def freeSpaceM(fname):
    f = os.statvfs(fname)
    size = (f[statvfs.F_BSIZE] * f[statvfs.F_BAVAIL]) / (1024*1024)
    return size


def checkSpace():
    dryRun()

    # check tmp dir
    avail = freeSpaceM(Config.tmp_dir)
    if avail < GL.backupSize:
        print "Insufficient space in %s: %d MB, %d MB is needed." % (Config.tmp_dir, avail, GL.backupSize)
        sys.exit(1)
    else:
        print "Available space of %s: %d MB." % (Config.tmp_dir, avail)

    # check target dir
    avail = freeSpaceM(GL.args.filename)
    dname = os.path.dirname(os.path.realpath(GL.args.filename))
    if avail < GL.backupSize:
        print "Insufficient space in %s: %d MB, %d MB is needed." % (dname, avail, GL.backupSize)
        sys.exit(1)
    else:
        print "Available space of %s: %d MB." % (dname, avail)


def main():
    try:
        thetime = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        parse_arguments(thetime)

        # setup the temp_dir
        Config.init(GL.args.temp_dir)

        if GL.args.dry_run:
            print 'Calculating free space needed for backup'
            dryRun()
            sys.exit()

        print 'Running validations'
        validations = [ValidationRunner(parseVRunnerArgs(["zenpack"])).run()]
        if any(validations):
            print 'Validation(s) failed, aborting export process'
            sys.exit(1)

        tar_file = GL.args.filename

        # add .tar extension
        fbase, fext = os.path.splitext(tar_file)
        if fext != '.tar':
            tar_file = tar_file + '.tar'
            GL.args.filename = tar_file

        # check accessibility of the tar file
        print 'Checking accessibility of %s ...' % tar_file
        if os.path.isfile(tar_file):
            os.remove(tar_file)
        try:
            with open(tar_file, 'w'):
                print '%s is accessible ...' % tar_file
        except:
            print 'Cannot open %s! please check accessibility ...' % tar_file
            sys.exit(1)

        if not os.path.isdir(Config.backup_dir):
            os.makedirs(Config.backup_dir)

        GL.dmd = ZenScriptBase(noopts=True, connect=True).dmd

        print 'Checking platform ...'
        try:
            ucsx = None
            ucsx = GL.dmd.ZenPackManager.packs._getOb('ZenPacks.zenoss.UCSXSkin')
        except:
            pass

        if ucsx:
            if ucsx.version in Config.ucsx_vers:
                print 'UCSPM version %s is supported.' % ucsx.version
            else:
                print 'UCSPM version %s is not supported ...' % ucsx.version
                sys.exit(1)

        # always do a dryRun to make sure the tmp and target dirs have
        # sufficient space
        checkSpace()

        remote_backups = backup_remote_collectors(thetime, Config.backup_dir)
        master_backup = backup_master(Config.backup_dir)

        export_component_list()
        export_dmduuid()
        genmd5(master_backup)
        make_export_tar(GL.args.filename, Config.components_filename, remote_backups, master_backup, Config.flexera_dir)

    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print str(e)
        cleanup(error=True)
        sys.exit(1)

    finally:
        cleanup(error=False)

if __name__ == '__main__':
    main()
