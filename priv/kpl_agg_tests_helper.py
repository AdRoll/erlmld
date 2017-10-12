#!/usr/bin/env python

"""
Generate test data used in Erlang tests.
"""

import aws_kinesis_agg.aggregator
import md5


def make_erlang_binary(data):
    return "<<{}>>".format(",".join(str(ord(byte)) for byte in data))


def print_agg_record(record):
    agg_pk, agg_ehk, agg_data = record.get_contents()
    agg_data_pretty = make_erlang_binary(agg_data)
    if len(agg_data_pretty) > 1000:
        agg_data_pretty = '{} ... {}'.format(
            agg_data_pretty[:500], agg_data_pretty[-500:])
    md5_calc = md5.new()
    md5_calc.update(agg_data)
    agg_data_md5 = make_erlang_binary(md5_calc.digest())
    print "Aggregated record: pk={} ehk={} data={} data_md5={}".format(
        agg_pk, agg_ehk, agg_data_pretty, agg_data_md5)


def simple_aggregation_test():
    agg = aws_kinesis_agg.aggregator.RecordAggregator()
    agg.add_user_record("pk1", "data1", "ehk1")
    agg.add_user_record("pk2", "data2", "ehk2")
    return [agg.clear_and_get()]


def shared_key_test():
    agg = aws_kinesis_agg.aggregator.RecordAggregator()
    for pk, data, ehk in [
            ("alpha", "data1", "zulu"),
            ("beta", "data2", "yankee"),
            ("alpha", "data3", "xray"),
            ("charlie", "data4", "yankee"),
            ("beta", "data5", "zulu")]:
        agg.add_user_record(pk, data, ehk)
    return [agg.clear_and_get()]


def record_fullness_test():
    agg = aws_kinesis_agg.aggregator.RecordAggregator()
    agg_records = []
    result = agg.add_user_record("pk1", "X" * 500000, "ehk1")
    assert result is None
    result = agg.add_user_record("pk2", "Y" * 600000, "ehk2")
    assert result is not None
    agg_records.append(result)
    result = agg.add_user_record("pk3", "Z" * 200000, "ehk3")
    assert result is None
    agg_records.append(agg.clear_and_get())
    return agg_records


if __name__ == "__main__":
    test_funcs = [
        simple_aggregation_test,
        shared_key_test,
        record_fullness_test,
    ]
    for test_func in test_funcs:
        records = test_func()
        print "{} made {} records:".format(test_func.__name__, len(records))
        for record in records:
            print_agg_record(record)
        print
