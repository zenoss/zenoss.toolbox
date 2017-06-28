The files in this directory are the work-in-progress on a tool to query Kibana for messages with the 'mtrace' tag, created by the 'Metric Tracer Bullets' feature (see [ZEN-27694](https://jira.zenoss.com/browse/ZEN-27694)/[ZEN-27696](https://jira.zenoss.com/browse/ZEN-27696))

The scripts in this directory come from two different parts of the project.

# Initial Scripts

`findgaps6.py`, was used to query Hong's perf lab box to see if any points were missing. (the 6 on findgaps6 is because it's the sixth saved iteration of that script. It should be renamed for production use). It does not use the tracer bullet functionality. It was used to look for missing datapoints as that functionality was being developed.

`parse_fg_out.py` and `check_missing.py` were developed to take the output of `findgaps6.py` and use it to query the trace messages from Kibana.

## findgaps6.py
`get_metric_names()` gets the names of metrics matching `pattern` from OpenTSDB. Those names are then passed to `process_metrics()`, which iterates through all of the names.

For each metric, it queries all datapoints in the time period from `DAYS_BACK` days in the past until the current time. It then looks through those points for gaps. It assumes a 5 minute (300 second) collection interval, and looks for gaps of more than `mingap` and less than `maxgap` missed intervals. It allows a 'fudge factor' around the gap size, (currently hard-coded in `GapFinder.has_gaps()` as +/-0.2).

## parse_fg_out.py
`parse_fg_out.py` is a script that was used to parse the output of findgaps6 and print output that can be used to feed `check_missing.py`. It appears that its functionality has been rolled into `check_missing.py`, so it may no longer be needed. Checking it in here, just in case it's useful.

## check_missing.py
`check_missing.py` iterates through the output of `findgaps6.py` and queries the Kibana logs for those messages, looking for 'tracer bullet' log messages. It uses `kquery.py`, which is documented in the next section, to query Kibana.

# Other scripts

## findgaps.py
`findgaps.py` is the main script for the second set of scripts. Its goal is to query the Logstash/Kibana logs and look for metrics with tracers on them (those having a `mtrace` tag). It should then collate the log messages by metric and timestamp, and flag any metrics that are missing expected messages (possibly indicating that those points may have encountered difficulty) or that have unexpected error/warning messages.

## kquery.py
`kquery.py` contains code for querying Kibana. The current implementation uses a version of the query that the Kibana web interface uses. The primary reason this project is on hold for the time being is that the web call will not return more than 10000 results (that's the maximum value for the `size` parameter (see `qt` in `get_query_string()` that will return messages). On a development image with a moderate to large number of traced metrics, that covers a time period of less than five minutes.

We'll probably need to go back and figure out how to query Logstash directly, rather than going through Kibana. The web interface tends to be very picky, so this is a bit of a challenge to get right. Still, it's probably the best way forward.

## mtrace.py
`mtrace.py` contains the code for parsing fields out of the various log formats. It will be used by `findgaps.py` as a prerequisite to collating the messages returned from Logstash/Kibana.
