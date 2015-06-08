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

import Globals
from Products.ZenUtils.Utils import unused
unused(Globals)
from Products.ZenUtils.ZenScriptBase import ZenScriptBase


class Config:
    tmp_dir =               '/tmp'
    dmd_uuid_filename =     '/tmp/dmd_uuid.txt'
    components_filename =   '/tmp/componentList.txt'


dmd = ZenScriptBase(noopts=True, connect=True).dmd


def run_dmd(script_text, output_file):
    script_file = tempfile.NamedTemporaryFile(delete=False, dir=Config.tmp_dir)
    try:
        script_file.write(script_text)
        script_file.close()
        subprocess.call(['zendmd', '--script=%s' % script_file.name], stdout=output_file, stdin=subprocess.PIPE, stderr=None)
    finally:
        os.unlink(script_file.name)


def parse_arguments(thetime):
    parser = argparse.ArgumentParser(description="4.x export script")
    default_export_filename = '4x-export-%s.tar' % thetime
    parser.add_argument('-f', '--filename', help='specify name of export file. export is created in the current directory. if unspecified, name is 4x-export-YYmmdd-HHMMSS.tar', default=default_export_filename)
    parser.add_argument('-z', '--no-zodb', help="don't backup zodb.", action='store_const', const=1)
    parser.add_argument('-e', '--no-eventsdb', help="don't backup events.", action='store_const', const=1)
    parser.add_argument('-p', '--no-perfdata', help="don't backup perf data (won't backup remote collectors unnecessarily).", action='store_const', const=1)
    args = parser.parse_args()
    return args


def get_collector_list():
    if not hasattr(dmd.Monitors, 'Hub'):
        print 'Not using distributed collectors.'
        return []
    colldict = {}
    for hub in dmd.Monitors.Hub.objectSubValues():
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
        for dev in dmd.Devices.getSubDevices():
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
        fp.write(dmd.uuid + '\n')
    print 'dmd uuid exported'


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

    backup_dir = os.path.join(os.environ['ZENHOME'], 'backups')
    if not os.path.isdir(backup_dir):
        os.makedirs(backup_dir)

    flexera_dir = os.path.join(os.environ['ZENHOME'], 'var', 'flexera')
    remote_backups = backup_remote_collectors(args, thetime, backup_dir)
    master_backup = backup_master(backup_dir, args)

    export_component_list()
    export_dmduuid()
    make_export_tar(args.filename, Config.components_filename, remote_backups, master_backup, flexera_dir)

if __name__ == '__main__':
    main()
