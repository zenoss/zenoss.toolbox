##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "0.9.5"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

import argparse
import datetime
import Globals
import logging
import math
import MySQLdb
import os
import re
import socket
import string
import subprocess
import sys
import time
import traceback
import transaction

from collections import OrderedDict
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from ZODB.transact import transact


def configure_logging(scriptname):
    '''Configure logging for zenoss.toolbox tool usage'''
    # Confirm /tmp, $ZENHOME and check for $ZENHOME/log/toolbox (create if needed)
    if not os.path.exists('/tmp'):
        print "/tmp doesn't exist - aborting"
        exit(1)
    zenhome_path = os.getenv("ZENHOME")
    if not zenhome_path:
        print "$ZENHOME undefined - are you running as the zenoss user?"
        exit(1)
    log_file_path = os.path.join(zenhome_path, 'log', 'toolbox')
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    # Setup "trash" toolbox log file (needed for ZenScriptBase log overriding)
    logging.basicConfig(filename='/tmp/toolbox.log.tmp', filemode='w', level=logging.INFO)
    # Create full path filename string for logfile, create RotatingFileHandler
    toolbox_log = logging.getLogger("%s" % (scriptname))
    toolbox_log.setLevel(logging.INFO)
    log_file_name = os.path.join(zenhome_path, 'log', 'toolbox', '%s.log' % (scriptname))
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=8192*1024, backupCount=5)
    # Set logging.Formatter for format and datefmt, attach handler
    formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    toolbox_log.addHandler(handler)
    # Print initialization string to console, log status to logfile
    toolbox_log.info("############################################################")
    print("\n[%s] Initializing %s version %s (detailed log at %s)\n" %
          (time.strftime("%Y-%m-%d %H:%M:%S"), scriptname, scriptVersion, log_file_name))
    toolbox_log.info("Initializing %s (version %s)", scriptname, scriptVersion)
    return toolbox_log


def get_lock(process_name, log):
    '''Global lock function to keep multiple tools from running at once'''
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        log.debug("Acquired '%s' execution lock" % (process_name))
    except socket.error:
        print("[%s] Unable to acquire %s socket lock - are other tools already running?\n" %
              (time.strftime(TIME_FORMAT), process_name))
        log.error("'%s' lock already exists - unable to acquire - exiting" % (process_name))
        log.info("############################################################")
        return False
    return True


def inline_print(message):
    '''Print message on a single line using sys.stdout.write, .flush'''
    sys.stdout.write("\r%s" % (message))
    sys.stdout.flush()


def parse_global_conf(filename, log):
    '''Parse the contesnts of $ZENHOME/etc/global.conf into a dict for connection info'''
    COMMENT_DELIMETER = '#'
    OPTION_DELIMETER = ' '
    parsed_options = {}
    log.info("Parsing $ZENHOME/etc/global.conf for database connection information")
    global_conf_file = open(filename)
    for line in global_conf_file:
        if COMMENT_DELIMETER in line:
            line, comment = line.split(COMMENT_DELIMETER, 1)
        if OPTION_DELIMETER in line:
            option, value = line.split(OPTION_DELIMETER, 1)
            option = option.strip()
            value = value.strip()
            parsed_options[option] = value
            log.debug("(%s %s)" % (option, parsed_options[option]))
    global_conf_file.close()
    log.debug("Parsing of $ZENHOME/etc/global.conf complete")
    return parsed_options


def log_zends_conf(filename, log):
    log.info("Logging $ZENDSHOME/etc/zends.cnf for review")
    zends_cnf_file = open(filename)
    for line in zends_cnf_file:
        log.info(line.strip())
    zends_cnf_file.close()


def connect_to_mysql(global_conf_dict, log):
    log.info("Opening connection to MySQL/ZenDS at %s" % global_conf_dict['zodb-host'])
    try:
        if os.environ['ZENDSHOME']:   # If ZENDSHOME is set, assume running with ZenDS
            if global_conf_dict['zodb-host'] == 'localhost':
                mysql_connection = MySQLdb.connect(unix_socket=global_conf_dict['zodb-socket'],
                                                   user=global_conf_dict['zodb-user'],
                                                   passwd=global_conf_dict['zodb-password'],
                                                   db=global_conf_dict['zodb-db'])
            else:
                mysql_connection = MySQLdb.connect(host=global_conf_dict['zodb-host'], port=int(global_conf_dict['zodb-port']),
                                                   user=global_conf_dict['zodb-admin-user'],
                                                   passwd=global_conf_dict['zodb-admin-password'],
                                                   db=global_conf_dict['zodb-db'])
        else:    # Assume MySQL (with no customized zodb-socket)
            mysql_connection = MySQLdb.connect(host=global_conf_dict['zodb-host'], port=int(global_conf_dict['zodb-port']),
                                               user=global_conf_dict['zodb-user'],
                                               passwd=global_conf_dict['zodb-password'],
                                               db=global_conf_dict['zodb-db'])
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        log.error(e)
        sys.exit(1)
    except Exception as e:
        print "Exception encountered: ", e
        log.error(e)
        sys.exit(1)

    return mysql_connection


def gather_MySQL_statistics(mysql_connection, log):
    # Execute point in time queries and parse results; return results in results_dict
    results_dict = {}
    try:
        mysql_cursor = mysql_connection.cursor()

        # INNODB: Gather results of "SHOW ENGINE INNODB STATUS"
        log.info("  Gathering results for 'SHOW ENGINE INNODB STATUS'")
        mysql_cursor.execute("SHOW ENGINE INNODB STATUS")
        innodb_results = mysql_cursor.fetchall()
        log.debug(innodb_results[0][2])

        # INNODB: Grab data for "History list length"
        history_list_length_location = string.find(innodb_results[0][2], 'History list length ')
        if history_list_length_location != -1:
            history_list_length_value = int(string.split(innodb_results[0][2][history_list_length_location+len('History list length '):], '\n')[0])
            results_dict['history_list_length'] = history_list_length_value
        else:
            log.error("Unable to find 'History List Length' in INNODB output")
            print "Unable to find 'History List Length' in INNODB output"
            sys.exit(1)

        # INNODB: Grab data for "TRANSACTION.*ACTIVE"
        active_transactions = re.findall("---TRANSACTION.*ACTIVE", innodb_results[0][2])
        results_dict['number_active_transactions'] = len(active_transactions)

        # INNODB: Grab data for "TRANSACTION.*ACTIVE" > 100 secs
        active_transactions_over = re.findall("---TRANSACTION.*ACTIVE [0-9]{3,} sec", innodb_results[0][2])
        results_dict['number_active_transactions_over'] = len(active_transactions_over)

        # Gather results and grab data for "Buffer Pool Percentage Used"
        mysql_cursor.execute("SELECT FORMAT(DataPages*100.0/TotalPages,2) FROM \
            (SELECT variable_value DataPages FROM information_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_pages_data') AS A, \
            (SELECT variable_value TotalPages FROM information_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_pages_total') AS B")

        results_dict['buffer_pool_used_percentage'] = float(mysql_cursor.fetchone()[0])

    except Exception as e:
        print "Exception encountered: ", e
        log.error(e)
        exit(1)

    log.info("  Results: %s" % (results_dict))

    return results_dict


def log_MySQL_variables(mysql_connection, log):
    """Takes mysql_connection and log and attempts to gather 'SHOW VARIABLES' results"""
    try:
        mysql_cursor = mysql_connection.cursor()
        log.info("  Gathering results for 'SHOW VARIABLES'")
        mysql_cursor.execute("SHOW VARIABLES")
        mysql_results = mysql_cursor.fetchall()
        for item in mysql_results:
            log.info(item)
    except Exception as e:
        print "Exception encountered: ", e
        log.error(e)
        exit(1)


def parse_options():
    """Defines command-line options and defaults for script """
    parser = argparse.ArgumentParser(version=scriptVersion, description="Gathers performance-related information "
                                     "about your database.  Documentation at https://support.zenoss.com/hc/en-us/articles/TBD")
    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (NOTE: lots of output)")
    parser.add_argument("-n", "-t", "--times", action="store", default=1, type=int,
                        help="number of times to gather data")
    parser.add_argument("-g", "--gap", action="store", default=60, type=int,
                        help="gap between gathering subsequent datapoints")
    parser.add_argument("-l3", "--level3", action="store_true", default=False,
                        help="Data gathering for L3 (standardized parameters)")
    return vars(parser.parse_args())


def main():
    '''Gathers metrics and statistics about the database that Zenoss uses for Zope.'''

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('zencheckdbstats')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    # Attempt to get the zenoss.toolbox.checkdbstats lock before any actions performed
    if not get_lock("zenoss.toolbox.checkdbstats", log):
        sys.exit(1)

    # Load up the contents of global.conf for using with MySQL
    global_conf_dict = parse_global_conf(os.environ['ZENHOME'] + '/etc/global.conf', log)

    # If running in debug, grab 'SHOW VARIABLES' and zends.cnf, if straightforward (localhost)
    if cli_options['debug']:
        if global_conf_dict['zodb-host'] == 'localhost':
            log_zends_conf(os.environ['ZENDSHOME'] + '/etc/zends.cnf', log)
        try:
            mysql_connection = connect_to_mysql(global_conf_dict, log)
            log_MySQL_variables(mysql_connection, log)
        except Exception as e:
            print "Exception encountered: ", e
            log.error(e)
            exit(1)
        finally:
            if mysql_connection:
                mysql_connection.close()
            log.info("Closed connection to MySQL/ZenDS at %s" % global_conf_dict['zodb-host'])

    sample_count = 0
    mysql_results_list = []

    if cli_options['level3']:
        cli_options['times'] = 120
        cli_options['gap'] = 60
        cli_options['debug'] = True

    while sample_count < cli_options['times']:
        sample_count += 1
        current_time = time.time()
        inline_print("[%s] Gathering MySQL/ZenDS metrics... (%d/%d)" %
                     (time.strftime(TIME_FORMAT), sample_count, cli_options['times']))
        try:
            mysql_connection = connect_to_mysql(global_conf_dict, log)
            mysql_results = gather_MySQL_statistics(mysql_connection, log)
        except Exception as e:
            print "Exception encountered: ", e
            log.error(e)
            exit(1)
        finally:
            if mysql_connection:
                mysql_connection.close()
            log.info("Closed connection to MySQL/ZenDS at %s" % global_conf_dict['zodb-host'])
        mysql_results_list.append((current_time, mysql_results))
        if sample_count < cli_options['times']:
            time.sleep(cli_options['gap'])

    # Process and display results (calculate statistics)
    print("\n\n[%s] Results:" % (time.strftime(TIME_FORMAT)))
    log.info("[%s] Final Results:" % (time.strftime(TIME_FORMAT)))
    observed_results_dict = OrderedDict([])
    observed_results_dict['History List Length'] = [item[1]['history_list_length'] for item in mysql_results_list]
    observed_results_dict['Bufferpool Used (%)'] = [item[1]['buffer_pool_used_percentage'] for item in mysql_results_list]
    observed_results_dict['ACTIVE TRANSACTIONS'] = [item[1]['number_active_transactions'] for item in mysql_results_list]
    observed_results_dict['ACTIVE TRANS > 100s'] = [item[1]['number_active_transactions_over'] for item in mysql_results_list]

    for key in observed_results_dict:
        values = observed_results_dict[key]
        if min(values) != max(values):
            output_message = "[{}]  {}: {:<10} (Average {:.2f}, Minimum {}, Maximum {})".format(time.strftime(TIME_FORMAT), key, values[-1], float(sum(values)/len(values)), min(values), max(values))
        else:
            output_message = "[{}]  {}: {}".format(time.strftime(TIME_FORMAT), key, values[-1])

        print output_message
        log.info(output_message)

    # Print final status summary, update log file with termination block
    print("\n[%s] Execution finished in %s\n" % (time.strftime(TIME_FORMAT),
                                                 datetime.timedelta(seconds=int(math.ceil(time.time() - execution_start)))))
    log.info("zencheckdbstats completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")
    sys.exit(0)


if __name__ == "__main__":
    main()
