##############################################################################
#
# Copyright (C) Zenoss, Inc. 2016, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################
#!/opt/zenoss/bin/python

scriptVersion = "2.0.0"
scriptSummary = " - gathers performance information about your DB - "
documentationURL = "https://support.zenoss.com/hc/en-us/articles/208050803"

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

import argparse
import datetime
import Globals
import logging
import math
import MySQLdb
import os
import re
import string
import subprocess
import sys
import time
import traceback
import transaction
import ZenToolboxUtils

from collections import OrderedDict
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from ZenToolboxUtils import inline_print
from ZODB.transact import transact


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


def connect_to_mysql(database_dict, log):
    log.info("Opening connection to MySQL/ZenDS for database %s at %s" % (database_dict['prettyName'], database_dict['host']))
    try:
        if os.environ.get('ZENDSHOME'):   # If ZENDSHOME is set, assume running with ZenDS
            if database_dict['host'] == 'localhost':
                mysql_connection = MySQLdb.connect(unix_socket=database_dict['socket'],
                                                   user=database_dict['admin-user'],
                                                   passwd=database_dict['admin-password'],
                                                   db=database_dict['database'])
            else:
                mysql_connection = MySQLdb.connect(host=database_dict['host'], port=int(database_dict['port']),
                                                   user=database_dict['admin-user'],
                                                   passwd=database_dict['admin-password'],
                                                   db=database_dict['database'])
        else:    # Assume MySQL (with no customized zodb-socket)
            mysql_connection = MySQLdb.connect(host=database_dict['host'], port=int(database_dict['port']),
                                               user=database_dict['admin-user'],
                                               passwd=database_dict['admin-password'],
                                                   db=database_dict['database'])
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


def main():
    '''Gathers metrics and statistics about the database that Zenoss uses for Zope/ZEP.'''

    execution_start = time.time()
    scriptName = os.path.basename(__file__).split('.')[0]
    parser = ZenToolboxUtils.parse_options(scriptVersion, scriptName + scriptSummary + documentationURL)
    # Add in any specific parser arguments for %scriptName
    parser.add_argument("-n", "-t", "--times", action="store", default=1, type=int,
                        help="number of times to gather data")
    parser.add_argument("-g", "--gap", action="store", default=60, type=int,
                        help="gap between gathering subsequent datapoints")
    parser.add_argument("-l3", "--level3", action="store_true", default=False,
                        help="Data gathering for L3 (standardized parameters)")
    cli_options = vars(parser.parse_args())
    log, logFileName = ZenToolboxUtils.configure_logging(scriptName, scriptVersion, cli_options['tmpdir'])
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    print "\n[%s] Initializing %s v%s (detailed log at %s)" % \
          (time.strftime("%Y-%m-%d %H:%M:%S"), scriptName, scriptVersion, logFileName)

    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not ZenToolboxUtils.get_lock("zenoss.toolbox.checkdbstats", log):
        sys.exit(1)

    if cli_options['level3']:
        cli_options['times'] = 120
        cli_options['gap'] = 60
        cli_options['debug'] = True
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    # Load up the contents of global.conf for using with MySQL
    global_conf_dict = parse_global_conf(os.environ['ZENHOME'] + '/etc/global.conf', log)

    # ZEN-19373: zencheckdbstats needs to take into account split databases
    databases_to_examine = []
    intermediate_dict = {}
    intermediate_dict['prettyName'] = "'zodb' Database"
    intermediate_dict['socket'] = global_conf_dict['zodb-socket']
    intermediate_dict['host'] = global_conf_dict['zodb-host']
    intermediate_dict['port'] = global_conf_dict['zodb-port']
    intermediate_dict['admin-user'] = global_conf_dict['zodb-admin-user']
    intermediate_dict['admin-password'] = global_conf_dict['zodb-admin-password']
    intermediate_dict['database'] = global_conf_dict['zodb-db']
    intermediate_dict['mysql_results_list'] = []
    databases_to_examine.append(intermediate_dict)
    if global_conf_dict['zodb-host'] != global_conf_dict['zep-host']:
        intermediate_dict = {}
        intermediate_dict['prettyName'] = "'zenoss_zep' Database"
        intermediate_dict['socket'] = global_conf_dict['zodb-socket']
        intermediate_dict['host'] = global_conf_dict['zep-host']
        intermediate_dict['port'] = global_conf_dict['zep-port']
        intermediate_dict['admin-user'] = global_conf_dict['zep-admin-user']
        intermediate_dict['admin-password'] = global_conf_dict['zep-admin-password']
        intermediate_dict['database'] = global_conf_dict['zep-db']
        intermediate_dict['mysql_results_list'] = []
        databases_to_examine.append(intermediate_dict)

    # If running in debug, log global.conf, grab 'SHOW VARIABLES' and zends.cnf, if straightforward (localhost)
    if cli_options['debug']:
        if global_conf_dict['zodb-host'] == 'localhost':
            log_zends_conf(os.environ['ZENDSHOME'] + '/etc/zends.cnf', log)
        try:
            for item in databases_to_examine:
                mysql_connection = connect_to_mysql(item, log)
                log_MySQL_variables(mysql_connection, log)
                if mysql_connection:
                    mysql_connection.close()
                    log.info("Closed connection to MySQL/ZenDS for database %s at %s" % (item['prettyName'], item['host']))
        except Exception as e:
            print "Exception encountered: ", e
            log.error(e)
            exit(1)

    sample_count = 0
    mysql_results_list = []

    while sample_count < cli_options['times']:
        sample_count += 1
        current_time = time.time()
        inline_print("[%s] Gathering MySQL/ZenDS metrics... (%d/%d)" %
                     (time.strftime(TIME_FORMAT), sample_count, cli_options['times']))
        try:
            for item in databases_to_examine:
                mysql_connection = connect_to_mysql(item, log)
                mysql_results = gather_MySQL_statistics(mysql_connection, log)
                item['mysql_results_list'].append((current_time, mysql_results))
                if mysql_connection:
                    mysql_connection.close()
                    log.info("Closed connection to MySQL/ZenDS for database %s at %s" % (item['prettyName'], item['host']))
        except Exception as e:
            print "Exception encountered: ", e
            log.error(e)
            exit(1)
        if sample_count < cli_options['times']:
            time.sleep(cli_options['gap'])

    # Process and display results (calculate statistics)
    print ("")
    for database in databases_to_examine:
        print("\n[%s] Results for %s:" % (time.strftime(TIME_FORMAT), database['prettyName']))
        log.info("[%s] Final Results for %s:" % (time.strftime(TIME_FORMAT), database['prettyName']))
        observed_results_dict = OrderedDict([])
        observed_results_dict['History List Length'] = [item[1]['history_list_length'] for item in database['mysql_results_list']]
        observed_results_dict['Bufferpool Used (%)'] = [item[1]['buffer_pool_used_percentage'] for item in database['mysql_results_list']]
        observed_results_dict['ACTIVE TRANSACTIONS'] = [item[1]['number_active_transactions'] for item in database['mysql_results_list']]
        observed_results_dict['ACTIVE TRANS > 100s'] = [item[1]['number_active_transactions_over'] for item in database['mysql_results_list']]
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
    print("** Additional information and next steps at %s **\n" % documentationURL)
    log.info("zencheckdbstats completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")
    sys.exit(0)


if __name__ == "__main__":
    main()
