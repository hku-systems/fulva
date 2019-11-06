#!/usr/bin/env python

# Copyright (c) 2010-2016 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""Runs a migration of a master."""
from __future__ import division, print_function
from common import *
import cluster
import log
from optparse import OptionParser
import recoverymetrics
from config import hosts


def backup_migrate(num_servers,
                   num_clients,
                   object_size,
                   num_objects,
                   num_overwrites,
                   replicas,
                   coordinator_args='',
                   master_args='',
                   backup_args='',
                   dpdk_port=-1,
                   master_ram=None,
                   old_master_ram=None,
                   num_removals=0,
                   timeout=100,
                   log_level='NOTICE',
                   log_dir='logs',
                   transport='basic+infud',
                   verbose=False,
                   debug=False,
                   superuser=False):
    server_binary = '%s/server' % obj_path
    client_binary = '%s/apps/migrationBenchmark' % obj_path
    ensure_servers_bin = '%s/ensureServers' % obj_path

    args = {}
    args['num_servers'] = num_servers
    args['num_clients'] = num_clients
    args['replicas'] = replicas
    args['timeout'] = timeout
    args['log_level'] = log_level
    args['log_dir'] = log_dir
    args['transport'] = transport
    args['verbose'] = verbose
    args['debug'] = debug
    # args['coordinator_host'] = getOldMasterHost()
    args['coordinator_args'] = coordinator_args
    args['share_hosts'] = True
    # args['dpdk_port'] = dpdk_port
    args['client_hosts'] = []
    args['superuser'] = superuser

    num_hosts = len(hosts) - 2


    for i in range(num_clients):
        args['client_hosts'].append(hosts[-2 - (i % num_hosts)])

    if backup_args:
        args['backup_args'] += backup_args
    else:
        args['backup_args'] = ''

    # Allocate enough memory on recovery masters to handle several
    # recovery partitions (most recoveries will only have one recovery
    # partition per master, which is about 500 MB).
    args['master_args'] = '--totalMasterMemory=57344 --segmentFrames=46080 --maxCores=9 --maxNonVolatileBuffers=0 -d'
    if master_args:
        args['master_args'] += ' ' + master_args
    args['client'] = ('%s -l %s' %
                      (client_binary, log_level))

    cluster.run(**args)


def insist(*args, **kwargs):
    """Keep trying recoveries until the damn thing succeeds"""
    while True:
        try:
            return backup_migrate(*args, **kwargs)
        except KeyboardInterrupt, e:
            raise
        except Exception, e:
            print('Recovery failed:', e)
            print('Trying again...')
        time.sleep(0.1)


if __name__ == '__main__':
    parser = OptionParser(description=
                          'Run a backup migration on a RAMCloud cluster.',
                          usage='%prog [options] ...',
                          conflict_handler='resolve')
    parser.add_option('--backupArgs', metavar='ARGS', default='',
                      dest='backup_args',
                      help='Additional command-line arguments to pass to '
                           'each backup')
    parser.add_option('--coordinatorArgs', metavar='ARGS', default='',
                      dest='coordinator_args',
                      help='Additional command-line arguments to pass to the '
                           'cluster coordinator')
    parser.add_option('--debug', action='store_true', default=False,
                      help='Pause after starting servers but before running '
                           'to enable debugging setup')
    parser.add_option('-d', '--logDir', default='logs',
                      metavar='DIR',
                      dest='log_dir',
                      help='Top level directory for log files; the files for '
                           'each invocation will go in a subdirectory.')
    parser.add_option('-l', '--logLevel', default='NOTICE',
                      choices=['DEBUG', 'NOTICE', 'WARNING', 'ERROR', 'SILENT'],
                      metavar='L', dest='log_level',
                      help='Controls degree of logging in servers')
    parser.add_option('--masterArgs', metavar='ARGS', default='',
                      dest='master_args',
                      help='Additional command-line arguments to pass to '
                           'each master')
    parser.add_option('-r', '--replicas', type=int, default=3,
                      metavar='N',
                      help='Number of disk backup copies for each segment')
    parser.add_option('--servers', type=int, default=8,
                      metavar='N', dest='num_servers',
                      help='Number of hosts on which to run servers for use '
                           "as recovery masters; doesn't include crashed server")
    parser.add_option('--clients', type=int, default=1,
                      metavar='N', dest='num_clients',
                      help='Number of clients on which to run servers for use '
                           "as recovery masters; doesn't include crashed server")
    parser.add_option('-s', '--size', type=int, default=1024,
                      help='Object size in bytes')
    parser.add_option('-n', '--numObjects', type=int,
                      metavar='N', dest='num_objects', default=0,
                      help='Total number of objects on crashed master (default value '
                           'will create enough data for one partition on each recovery '
                           'master)')
    parser.add_option('-o', '--numOverwrite', type=int,
                      metavar='N', dest='num_overwrites', default=1,
                      help='Number of writes per key')
    parser.add_option('-t', '--timeout', type=int, default=1000,
                      metavar='SECS',
                      help="Abort if the client application doesn't finish within "
                           'SECS seconds')
    parser.add_option('-T', '--transport', default='infrc',
                      help='Transport to use for communication with servers')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
                      help='Print progress messages')
    parser.add_option('--masterRam', type=int,
                      metavar='N', dest='master_ram',
                      help='Megabytes to allocate for the log per recovery master')
    parser.add_option('--oldMasterRam', type=int,
                      metavar='N', dest='old_master_ram',
                      help='Megabytes to allocate for the log of the old master')
    parser.add_option('--removals', type=int,
                      metavar='N', dest='num_removals', default=0,
                      help='Perform this many removals after filling the old master')
    parser.add_option('--trend',
                      dest='trends', action='append',
                      help='Add to dumpstr trend line (may be repeated)')
    parser.add_option('--dpdkPort', type=int, dest='dpdk_port', default=0,
                      help='Ethernet port that the DPDK driver should use')
    parser.add_option('--superuser', action='store_true', default=False,
                      help='Start the cluster and clients as superuser')

    (options, args) = parser.parse_args()
    args = {}
    args['num_servers'] = options.num_servers
    args['num_clients'] = options.num_clients
    args['object_size'] = options.size
    if options.num_objects == 0:
        # The value below depends on Recovery::PARTITION_MAX_BYTES
        # and various other RAMCloud parameters
        options.num_objects = 480000 * options.num_servers
    args['num_objects'] = options.num_objects
    args['num_overwrites'] = options.num_overwrites
    args['replicas'] = options.replicas
    args['master_ram'] = options.master_ram
    args['old_master_ram'] = options.old_master_ram
    args['num_removals'] = options.num_removals
    args['timeout'] = options.timeout
    args['log_level'] = options.log_level
    args['log_dir'] = options.log_dir
    args['transport'] = options.transport
    args['verbose'] = options.verbose
    args['debug'] = options.debug
    args['coordinator_args'] = options.coordinator_args
    args['master_args'] = options.master_args
    args['backup_args'] = options.backup_args
    args['dpdk_port'] = options.dpdk_port
    args['superuser'] = options.superuser

    try:
        backup_migrate(**args)

    finally:
        log_info = log.scan("%s/latest" % (options.log_dir),
                            ["WARNING", "ERROR"],
                            ["Ping timeout", "Pool destroyed", "told to kill",
                             "is not responding", "failed to exchange",
                             "timed out waiting for response",
                             "received nonce", "Couldn't open session",
                             "verifying cluster membership"])
        if len(log_info) > 0:
            print(log_info)
