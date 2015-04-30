##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "0.9"

import argparse
import datetime
import os
import subprocess
import sys
import tempfile

list_components_dmd = '''
import sys
devcount = 0
for dev in dmd.Devices.getSubDevices():
    print '### components for %s' % '/'.join(dev.getPrimaryPath())
    for comp in dev.getMonitoredComponents():
        print '/'.join(comp.getPrimaryPath())
    devcount = devcount + 1
    if devcount % 100 is 0:
        sys.stderr.write('exported 100 devices\\n')

sys.stderr.write('a total of %d devices in export\\n' % devcount)
'''

get_remote_collector_hostnames_dmd = '''
import sys
if not hasattr(dmd.Monitors, 'Hub'):
    sys.stderr.write('Not using distributed collectors.\\n')
    sys.exit(0)
colldict = {}
for hub in dmd.Monitors.Hub.objectSubValues():
    for collector in hub.collectors():
        if collector.isLocalHost():
            continue
        if colldict.has_key(collector.hostname):
            sys.stderr.write('collector %s shares a hostname with collector %s, skipping duplicate.\\n' % (collector.id, colldict[collector.hostname].id))
            continue
        colldict[collector.hostname] = collector
if not colldict:
    sys.stderr.write('All collectors are local.\\n')
    sys.exit(0)
for collector in colldict.values():
    print ','.join([collector.hub().id, collector.id, collector.hostname])
'''

def run_dmd(script_text, output_file):
    script_file = tempfile.NamedTemporaryFile(delete=False, dir=os.getcwd())
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
    collector_file = tempfile.NamedTemporaryFile(delete=False, dir=os.getcwd())
    try:
        run_dmd(get_remote_collector_hostnames_dmd, collector_file)
    finally:
        collector_file.close()
    try:
        with open(collector_file.name, 'r') as f:
            collector_list = [l.strip() for l in f.readlines()]
    finally:
        os.unlink(collector_file.name)
    return collector_list


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
        scpcmd = ['scp', 'zenoss@%s:%s' % (hostname, remote_backup_fn), '.']
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


def export_component_list(components_filename):
    print 'exporting component list ...'
    components_fn = os.path.join(os.getcwd(), components_filename)
    components_file = open(components_fn, 'w')
    try:
        run_dmd(list_components_dmd, components_file)
    finally:
        components_file.close()
    print 'component list exported'


def make_export_tar(args, components_filename, remote_backups, master_backup_path):
    tarcmd = ['tar', 'cf', args.filename, components_filename]
    tarcmd.extend(remote_backups)
    tar_result = subprocess.call(tarcmd)
    if tar_result is not 0:
        print 'failed to create tarfile'
        sys.exit(tar_result)
    backup_split = os.path.split(master_backup_path)
    export_fn = os.path.join(os.getcwd(), args.filename)
    tar_result = subprocess.call(['tar', '-C', backup_split[0], '-uf', export_fn, backup_split[1]])
    if tar_result is not 0:
        print 'failed to add backup to tarfile'
        sys.exit(tar_result)
    print 'export successful. file is %s' % export_fn

def main():
    thetime = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    args = parse_arguments(thetime)
    backup_dir = os.path.join(os.environ['ZENHOME'], 'backups')
    remote_backups = backup_remote_collectors(args, thetime, backup_dir)
    master_backup = backup_master(backup_dir, args)
    components_filename = 'componentList.txt'
    export_component_list(components_filename)
    make_export_tar(args, components_filename, remote_backups, master_backup)

if __name__ == '__main__':
    main()
