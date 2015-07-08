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
import subprocess
import sys
import tempfile
import shutil

import Globals
from Products.ZenUtils.Utils import unused
unused(Globals)
from Products.ZenUtils.ZenScriptBase import ZenScriptBase


class Config:
    tmp_dir =               tempfile.mkdtemp()
    dmd_uuid_filename =     os.path.join(tmp_dir, 'dmd_uuid.txt')
    components_filename =   os.path.join(tmp_dir, 'componentList.txt')
    md5_filename =          os.path.join(tmp_dir, 'backup.md5')
    backup_dir =            os.path.join(os.environ['ZENHOME'], 'backups')
    flexera_dir =           os.path.join(os.environ['ZENHOME'], 'var', 'flexera')
    ucsx_vers =             ['1.1', '1.1.1']


class GL:
    dmd = 0
    args = 0


def parse_arguments(thetime):
    parser = argparse.ArgumentParser(description="4.x export script")
    default_export_filename = '4x-export-%s.tar' % thetime
    parser.add_argument('-f', '--filename', help='specify name of export file. export is created in the current directory. if unspecified, name is 4x-export-YYmmdd-HHMMSS.tar', default=default_export_filename)
    parser.add_argument('-z', '--no-zodb', help="don't backup zodb.", action='store_true', default=False)
    parser.add_argument('-e', '--no-eventsdb', help="don't backup events.", action='store_true', default=False)
    parser.add_argument('-p', '--no-perfdata', help="don't backup perf data (won't backup remote collectors unnecessarily).", action='store_true', default=False)
    parser.add_argument('-d', '--debug', help="debug mode", action='store_true', default=False)
    GL.args = parser.parse_args()
    return GL.args


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


def backup_remote_collectors(args, thetime, backup_dir):
    if args.no_perfdata:
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


def backup_master(backup_dir, args):
    print 'making new backup ...'
    before_dir = set(os.listdir(backup_dir))
    zbcommand = ['zenbackup']
    if args.no_zodb:
        zbcommand.append('--no-zodb')
    if args.no_eventsdb:
        zbcommand.append('--no-eventsdb')
    if args.no_perfdata:
        zbcommand.append('--no-perfdata')
    zbresult = subprocess.call(zbcommand)
    if zbresult is not 0:
        print 'no backup specified and making one failed, aborting ...'
        sys.exit(zbresult)
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


def main():
    thetime = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    args = parse_arguments(thetime)

    tar_file = args.filename

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
        print 'Non-UCSPM platform.'
        pass

    if ucsx:
        if ucsx.version in Config.ucsx_vers:
            print 'UCSPM version %s is supported.' % ucsx.version
        else:
            print 'UCSPM version %s is not supported ...' % ucsx.version
            sys.exit(1)

    remote_backups = backup_remote_collectors(args, thetime, Config.backup_dir)
    master_backup = backup_master(Config.backup_dir, args)

    export_component_list()
    export_dmduuid()
    genmd5(master_backup)
    make_export_tar(args.filename, Config.components_filename, remote_backups, master_backup, Config.flexera_dir)


if __name__ == '__main__':
    try:
        main()

    finally:
        # cleanup the temp dir
        if not GL.args.debug:
            shutil.rmtree(Config.tmp_dir)
