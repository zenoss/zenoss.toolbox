#!/usr/bin/env python
##############################################################################
#
# Copyright (C) Zenoss, Inc. 2017, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import datetime as dt
import unittest

import pytz

import mtrace

test_hits = [
    {
        u'sort': [1496287790976],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'fields': {
                u'instance': u'0',
                u'type': u'metricshipper',
                u'ccWorkerID': u'580a8879',
                u'service': u'32gzxsi69vjww9uqj9dt7iv1e'
            },
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'f04c848d149f',
                u'name': u'f04c848d149f'
            },
            u'@version': u'1',
            u'@timestamp': u'2017-06-01T03:29:50.976Z',
            u'host': u'f04c848d149f',
            u'file': u'/opt/zenoss/log/metricshipper.log',
            u'offset': 3192309,
            u'input_type': u'log',
            u'message': u'time="2017-06-01T03:29:45Z" level=info msg=sent elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 ',
            u'type': u'log'
        },
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {
            u'@timestamp': [1496287790976]
        },
        u'_id': u'AVxhs9RiKo-PUs7Y5iz4'},
    {
        u'sort': [1496287790975],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'f04c848d149f',
                u'name': u'f04c848d149f'
            },
            u'fields': {
                u'instance': u'0',
                u'type': u'metricshipper',
                u'ccWorkerID': u'580a8879',
                u'service': u'32gzxsi69vjww9uqj9dt7iv1e'
            },
            u'@timestamp': u'2017-06-01T03:29:50.975Z',
            u'offset': 3184505,
            u'host': u'f04c848d149f',
            u'file': u'/opt/zenoss/log/metricshipper.log',
            u'type': u'log',
            u'input_type': u'log',
            u'message': u'time="2017-06-01T03:29:45Z" level=info msg="metric read from redis" elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 ',
            u'@version': u'1'
        },
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {u'@timestamp': [1496287790975]},
        u'_id': u'AVxhs9QRKo-PUs7Y5izO'
    },
    {
        u'sort': [1496287790975],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'type': u'log',
            u'fields': {
                u'instance': u'0',
                u'type': u'metricshipper',
                u'ccWorkerID': u'580a8879',
                u'service': u'32gzxsi69vjww9uqj9dt7iv1e'},
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'f04c848d149f',
                u'name': u'f04c848d149f'
            },
            u'input_type': u'log',
            u'@timestamp': u'2017-06-01T03:29:50.975Z',
            u'host': u'f04c848d149f',
            u'file': u'/opt/zenoss/log/metricshipper.log',
            u'offset': 3188451,
            u'message': u'time="2017-06-01T03:29:45Z" level=info msg=publishing elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 ',
            u'@version': u'1'
        },
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {u'@timestamp': [1496287790975]},
        u'_id': u'AVxhs9QRKo-PUs7Y5izh'
    },
    {
        u'sort': [1496287785205],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'type': u'log',
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'3ca1cbecd5e8',
                u'name': u'3ca1cbecd5e8'
            },
            u'fields': {
                u'instance': u'2',
                u'type': u'zenperfsnmp',
                u'ccWorkerID': u'580a8879',
                u'service': u'8whykqhz8stylsd7ml08crf2n',
                u'monitor': u'localhost'
            },
            u'@timestamp': u'2017-06-01T03:29:45.205Z',
            u'host': u'3ca1cbecd5e8',
            u'file': u'/opt/zenoss/log/zenperfsnmp.log',
            u'offset': 5828502,
            u'input_type': u'log',
            u'message': u"2017-06-01 03:29:44,576 INFO zen.MetricWriter: mtrace: publishing metric switch_10_87_254_3/ifInErrors_ifInErrors 0 1496287784 {'device': 'switch_10_87_254_3', 'mtrace': '1496287784', 'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f', 'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1'}",
            u'@version': u'1'
        },
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {u'@timestamp': [1496287785205]},
        u'_id': u'AVxhs7sZKo-PUs7Y5h7g'
    },
    {
        u'sort': [1496287785195],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'77aa796be59b',
                u'name': u'77aa796be59b'
            },
            u'fields': {
                u'instance': u'0',
                u'type': u'metricconsumer',
                u'ccWorkerID': u'580a8879',
                u'service': u'8dkv4u5z35s24r02a4075nw3w'
            },
            u'@timestamp': u'2017-06-01T03:29:45.195Z',
            u'offset': 1723922,
            u'host': u'77aa796be59b',
            u'file': u'/opt/zenoss/log/metric-consumer-app.log',
            u'type': u'log',
            u'input_type': u'log',
            u'message': u'INFO  [2017-06-01 03:29:45,189] org.zenoss.app.consumer.metric.impl.OpenTsdbWriter: mtrace=1496287784 elapsed=1 message="Converted metric. Output string: "put switch_10_87_254_3/ifInErrors_ifInErrors 1496287784 0 contextUUID=2eae74be-ff98-496e-8446-d39d328a337f device=switch_10_87_254_3 key=Devices/switch_10_87_254_3/os/interfaces/Logical-int-1 mtrace=1 zenoss_tenant_id=39da4d40-ecc0-11e6-a9e9-0242ac11000a\\n"" metric=[Metric{metric=\'switch_10_87_254_3/ifInErrors_ifInErrors\', timestamp=1496287784, value=0.0, tags={mtrace=1496287784, zenoss_tenant_id=39da4d40-ecc0-11e6-a9e9-0242ac11000a, x-metric-consumer-client-id=websocket4136, contextUUID=2eae74be-ff98-496e-8446-d39d328a337f, device=switch_10_87_254_3, key=Devices/switch_10_87_254_3/os/interfaces/Logical-int-1}]',
            u'@version': u'1'},
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {u'@timestamp': [1496287785195]},
        u'_id': u'AVxhs8pMKo-PUs7Y5iXU'},
    {
        u'sort': [1496287785195],
        u'_type': u'log',
        u'_source': {
            u'count': 1,
            u'type': u'log',
            u'tags': [u'beats_input_codec_plain_applied'],
            u'beat': {
                u'hostname': u'77aa796be59b',
                u'name': u'77aa796be59b'
            },
            u'input_type': u'log',
            u'@timestamp': u'2017-06-01T03:29:45.195Z',
            u'offset': 1724687,
            u'host': u'77aa796be59b',
            u'file': u'/opt/zenoss/log/metric-consumer-app.log',
            u'fields': {
                u'instance': u'0',
                u'type': u'metricconsumer',
                u'ccWorkerID': u'580a8879',
                u'service': u'8dkv4u5z35s24r02a4075nw3w'
            },
            u'message': u"INFO  [2017-06-01 03:29:45,189] org.zenoss.app.consumer.metric.impl.OpenTsdbWriter: mtrace=1496287784 elapsed=1 message=\"Publishing metric\" metric=[Metric{metric='switch_10_87_254_3/ifInErrors_ifInErrors', timestamp=1496287784, value=0.0, tags={mtrace=1496287784, zenoss_tenant_id=39da4d40-ecc0-11e6-a9e9-0242ac11000a, x-metric-consumer-client-id=websocket4136, contextUUID=2eae74be-ff98-496e-8446-d39d328a337f, device=switch_10_87_254_3, key=Devices/switch_10_87_254_3/os/interfaces/Logical-int-1}]",
            u'@version': u'1'
        },
        u'_score': None,
        u'_index': u'logstash-2017.06.01',
        u'fields': {u'@timestamp': [1496287785195]},
        u'_id': u'AVxhs8pMKo-PUs7Y5iXW'
    },
]

test_messages = [
    (
        u'metricshipper',
        u'time="2017-06-01T03:29:45Z" level=info msg=sent elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 ',
    ),
    (
        u'metricshipper',
        u'time="2017-06-01T03:29:45Z" level=info msg="metric read from redis" elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 '
    ),
    (
        u'metricshipper',
        u'time="2017-06-01T03:29:45Z" level=info msg=publishing elapsed=1 metric="switch_10_87_254_3/ifInErrors_ifInErrors" mtrace=1496287784 tags=map[device:switch_10_87_254_3 mtrace:1496287784 contextUUID:2eae74be-ff98-496e-8446-d39d328a337f key:Devices/switch_10_87_254_3/os/interfaces/Logical-int-1] timestamp=1496287784 value=0 '
    ),
    (
        u'zenperfsnmp',
        u"2017-06-01 03:29:44,576 INFO zen.MetricWriter: mtrace: publishing metric switch_10_87_254_3/ifInErrors_ifInErrors 0 1496287784 {'device': 'switch_10_87_254_3', 'mtrace': '1496287784', 'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f', 'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1'}"
    ),
    (
        u'metricconsumer',
        u'INFO  [2017-06-06 21:58:32,609] org.zenoss.app.consumer.metric.impl.OpenTsdbWriter: mtrace=1496786310 elapsed=2 message="Converted metric. Output_string="put Cisco_10.171.100.13/ifOperStatus_ifOperStatus 1496786310 1 contextUUID=fb959964-3198-4805-8144-00de62cd7659 device=Cisco_10.171.100.13 key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan981 mtrace=1 zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017\n"" metric=[Metric{metric=\'Cisco_10.171.100.13/ifOperStatus_ifOperStatus\', timestamp=1496786310, value=1.0, tags={mtrace=1496786310, zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017, x-metric-consumer-client-id=websocket2, contextUUID=fb959964-3198-4805-8144-00de62cd7659, device=Cisco_10.171.100.13, key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan981}}]'
    ),
    (
        u'metricconsumer',
        u'INFO  [2017-06-06 21:58:32,613] org.zenoss.app.consumer.metric.impl.OpenTsdbWriter: mtrace=1496786310 elapsed=2 message="Publishing metric" metric=[Metric{metric=\'Cisco_10.171.100.13/vtpVlanState_vtpVlanState\', timestamp=1496786310, value=1.0, tags={mtrace=1496786310, zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017, x-metric-consumer-client-id=websocket2, contextUUID=e5fe20b2-4132-4447-9719-eeeb6c0c2f0e, device=Cisco_10.171.100.13, key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan487}}]'
    ),
    (
        u'zenperfsnmp',
        u"2017-06-14 16:42:52,858 INFO zen.MetricWriter: mtrace: publishing metric Cisco_10.171.100.13/cefcTotalAvailableCurrent_cefcTotalAvailableCurrent 6598 1497458572 {'device': 'Cisco_10.171.100.13', 'mtrace': '1497458572', 'contextUUID': '86d9c1a5-40ae-4641-857f-11770c91ae03', 'key': 'Devices/Cisco_10.171.100.13/hw/powersupplies/PS 2 WS-CAC-3000W'}"
    ),
]

class TestMtrace(unittest.TestCase):
    maxDiff = None

    def test_get_mtrace(self):
        cases = ((test_hits[0], '1496287784'),
                 (test_hits[1], '1496287784'),
                 (test_hits[2], '1496287784'),
                 (test_hits[3], '1496287784'),
                 (test_hits[4], '1496287784'),
                 (test_hits[5], '1496287784'))
        for x, (input, expected) in enumerate(cases):
            subj = mtrace.Mtrace(input)
            actual = subj.get_mtrace()
            self.assertEqual(actual, expected, x)

    def test_get_level(self):
        cases = ((test_hits[0], "INFO"),
                 (test_hits[1], "INFO"),
                 (test_hits[2], "INFO"),
                 (test_hits[3], "INFO"),
                 (test_hits[4], "INFO"),
                 (test_hits[5], "INFO"))
        for x, (input, expected) in enumerate(cases):
            subj = mtrace.Mtrace(input)
            actual = subj.get_level()
            self.assertEqual(actual, expected, x)

    def test_get_fieldtype(self):
        cases = ((test_hits[0], u'metricshipper'),
                 (test_hits[1], u'metricshipper'),
                 (test_hits[2], u'metricshipper'),
                 (test_hits[3], u'zenperfsnmp'),
                 (test_hits[4], u'metricconsumer'),
                 (test_hits[5], u'metricconsumer'))
        for x, (input, expected) in enumerate(cases):
            subj = mtrace.Mtrace(input)
            actual = subj.get_fieldtype()
            self.assertEqual(actual, expected, x)

    def test_get_msg_datetime(self):
        # 1496287785 => GMT: Thursday, June 1, 2017 3:29:45 AM
        cases = (
            (test_hits[0], dt.datetime(2017, 06, 01, 03, 29, 45, 0, pytz.UTC)),
            (test_hits[1], dt.datetime(2017, 06, 01, 03, 29, 45, 0, pytz.UTC)),
            (test_hits[2], dt.datetime(2017, 06, 01, 03, 29, 45, 0, pytz.UTC)),
            (
                test_hits[3],
                dt.datetime(2017, 06, 01, 03, 29, 44, 576000, pytz.UTC)),
            # dt.datetime.fromtimestamp(1496287784.576)),
            (
                test_hits[4],
                dt.datetime(2017, 06, 01, 03, 29, 45, 189000, pytz.UTC)),
            # dt.datetime.fromtimestamp(1496287785.189)),
            (
                test_hits[5],
                dt.datetime(2017, 06, 01, 03, 29, 45, 189000, pytz.UTC)),
            # dt.datetime.fromtimestamp(1496287785.189)),
        )

        for x, (input, expected) in enumerate(cases):
            subj = mtrace.Mtrace(input)
            actual = subj.get_msg_date()
            self.assertEqual(actual, expected, x)

    def test_parse_metricshipper_msg(self):
        cases = ((test_messages[0][1],
                  {
                      'logtime': '2017-06-01T03:29:45Z',
                      'log_datetime': dt.datetime(2017, 6, 1, 3, 29, 45, 0,
                                                  pytz.UTC),
                      'level': 'info',
                      'msg': 'sent',
                      'elapsed': '1',
                      'metric': 'switch_10_87_254_3/ifInErrors_ifInErrors',
                      'mtrace': '1496287784',
                      'tags':
                          {
                              'device': 'switch_10_87_254_3',
                              'mtrace': '1496287784',
                              'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f',
                              'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1'
                          },
                      'timestamp': '1496287784',
                      'value': '0'
                  }),
                 (test_messages[1][1],
                  {
                      'logtime': '2017-06-01T03:29:45Z',
                      'log_datetime': dt.datetime(2017, 6, 1, 3, 29, 45, 0,
                                                  pytz.UTC),
                      'level': 'info',
                      'msg': 'metric read from redis',
                      'elapsed': '1',
                      'metric': 'switch_10_87_254_3/ifInErrors_ifInErrors',
                      'mtrace': '1496287784',
                      'tags':
                          {
                              'device': 'switch_10_87_254_3',
                              'mtrace': '1496287784',
                              'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f',
                              'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1'
                          },
                      'timestamp': '1496287784',
                      'value': '0'
                  }),
                 (test_messages[2][1],
                  {
                      'logtime': '2017-06-01T03:29:45Z',
                      'log_datetime': dt.datetime(2017, 6, 1, 3, 29, 45, 0,
                                                  pytz.UTC),
                      'level': 'info',
                      'msg': 'publishing',
                      'elapsed': '1',
                      'metric': 'switch_10_87_254_3/ifInErrors_ifInErrors',
                      'mtrace': '1496287784',
                      'tags': {
                          'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f',
                          'device': 'switch_10_87_254_3',
                          'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1',
                          'mtrace': '1496287784'},
                      'timestamp': '1496287784',
                      'value': '0'
                  }))
        for x, (input, expected) in enumerate(cases):
            actual = mtrace.parse_metricshipper_message(input)
            self.assertEqual(actual, expected, x)

    def test_parse_metricconsumer_msg(self):
        cases = (
            (
                test_messages[4][1], {
                    'class': 'org.zenoss.app.consumer.metric.impl.OpenTsdbWriter',
                    'logtime': '2017-06-06 21:58:32,609',
                    'log_datetime': dt.datetime(2017, 6, 6, 21, 58, 32, 609000,
                                                pytz.UTC),
                    'elapsed': '2',
                    'level': 'INFO',
                    'message': '"Converted metric. Output_string="put Cisco_10.171.100.13/ifOperStatus_ifOperStatus 1496786310 1 contextUUID=fb959964-3198-4805-8144-00de62cd7659 device=Cisco_10.171.100.13 key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan981 mtrace=1 zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017""',
                    'metric': 'Metric{metric=\'Cisco_10.171.100.13/ifOperStatus_ifOperStatus\', timestamp=1496786310, value=1.0, tags={mtrace=1496786310, zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017, x-metric-consumer-client-id=websocket2, contextUUID=fb959964-3198-4805-8144-00de62cd7659, device=Cisco_10.171.100.13, key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan981}}',
                    'mtrace': '1496786310',
                    'tags': {
                        'mtrace': '1496786310',
                        'zenoss_tenant_id': '285cd032-342e-11e7-a769-0242ac110017',
                        'x-metric-consumer-client-id': 'websocket2',
                        'contextUUID': 'fb959964-3198-4805-8144-00de62cd7659',
                        'device': 'Cisco_10.171.100.13',
                        'key': 'Devices/Cisco_10.171.100.13/os/interfaces/Vlan981'
                    }
                }
            ),
            (
                test_messages[5][1], {
                    'class': 'org.zenoss.app.consumer.metric.impl.OpenTsdbWriter',
                    'logtime': '2017-06-06 21:58:32,613',
                    'log_datetime': dt.datetime(2017, 6, 6, 21, 58, 32, 613000,
                                                pytz.UTC),
                    'elapsed': '2',
                    'level': 'INFO',
                    'message': '"Publishing metric"',
                    'metric': "Metric{metric='Cisco_10.171.100.13/vtpVlanState_vtpVlanState', timestamp=1496786310, value=1.0, tags={mtrace=1496786310, zenoss_tenant_id=285cd032-342e-11e7-a769-0242ac110017, x-metric-consumer-client-id=websocket2, contextUUID=e5fe20b2-4132-4447-9719-eeeb6c0c2f0e, device=Cisco_10.171.100.13, key=Devices/Cisco_10.171.100.13/os/interfaces/Vlan487}}",
                    'mtrace': '1496786310',
                    'tags': {
                        'device': 'Cisco_10.171.100.13',
                        'contextUUID': 'e5fe20b2-4132-4447-9719-eeeb6c0c2f0e',
                        'key': 'Devices/Cisco_10.171.100.13/os/interfaces/Vlan487',
                        'mtrace': '1496786310',
                        'x-metric-consumer-client-id': 'websocket2',
                        'zenoss_tenant_id': '285cd032-342e-11e7-a769-0242ac110017'
                    }
                }))
        for x, (input, expected) in enumerate(cases):
            actual = mtrace.parse_metricconsumer_message(input)
            for (k, v) in expected.items():
                self.assertEqual(actual[k], expected[k])
            self.assertEqual(actual, expected, x)

    def test_parse_zenperfsnmp_msg(self):
        cases = (
            (test_messages[3][1], {
                'level': u'INFO',
                'class': u'zen.MetricWriter',
                'message': u'publishing',
                'logtime': u'2017-06-01 03:29:44,576',
                'log_datetime': dt.datetime(2017, 6, 1, 3, 29, 44, 576000,
                                            pytz.UTC),
                'metric': u'switch_10_87_254_3/ifInErrors_ifInErrors',
                'mtrace': '1496287784',
                'timestamp': '1496287784',
                'value': '0',
                'tags': {
                    'device': 'switch_10_87_254_3',
                    'mtrace': '1496287784',
                    'contextUUID': '2eae74be-ff98-496e-8446-d39d328a337f',
                    'key': 'Devices/switch_10_87_254_3/os/interfaces/Logical-int-1',
                }
            }),
            (test_messages[6][1], {
                'logtime': u'2017-06-14 16:42:52,858',
                'log_datetime': dt.datetime(2017, 6, 14, 16, 42, 52, 858000,
                                            pytz.UTC),
                'level': u'INFO',
                'class': u'zen.MetricWriter',
                'message': u'publishing',
                'metric': u'Cisco_10.171.100.13/cefcTotalAvailableCurrent_cefcTotalAvailableCurrent',
                'value': u'6598',
                'mtrace': '1497458572',
                'timestamp': u'1497458572',
                'tags': {
                    'device': 'Cisco_10.171.100.13',
                    'mtrace': '1497458572',
                    'contextUUID': '86d9c1a5-40ae-4641-857f-11770c91ae03',
                    'key': 'Devices/Cisco_10.171.100.13/hw/powersupplies/PS 2 WS-CAC-3000W',
                }
            })
        )
        for x, (input, expected) in enumerate(cases):
            actual = mtrace.parse_zenperfsnmp_message(input)
            for (k, v) in expected.items():
                self.assertEqual(actual[k], expected[k])
            self.assertEqual(actual, expected, x)
