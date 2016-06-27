import argparse
import json
import re
import sys

from elasticsearch import Elasticsearch


class ElasticSearchClient(object):
    def __init__(self):
        self.client = Elasticsearch()

    def index(self, run_type, action, num_servers,
              total_time, avg_runtime, timestamp):
        doc = {
            "action": action,
            "num_servers": num_servers,
            "total_time": total_time,
            "avg_runtime": avg_runtime,
            "timestamp": timestamp}
        self.client.index(
            index="{0}-benchmark-index".format(run_type),
            doc_type='results', body=doc)


def parse_pkb_output(output):
    regex_starts = [m.start() for m in re.finditer("{'metadata'", output)]
    regex_ends = [
        regex_starts[1],
        output[regex_starts[1]:].index("\n\n\n")+regex_starts[1]]
    json_outputs = [
        output[regex_starts[0]:regex_ends[0]],
        output[regex_starts[1]:regex_ends[1]]]
    json_outputs = [
        json.loads(o.replace("'", '"').replace("False", "false").strip())
        for o in json_outputs]
    num_servers = [o.get("metadata").get("vm_count") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    total_time = [o.get("value") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    avg_runtime = total_time / num_servers
    timestamp = [o.get("timestamp") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    return {"action": "create", "num_servers": num_servers,
            "total_time": total_time, "avg_runtime": avg_runtime,
            "timestamp": timestamp}


class ArgumentParser(argparse.ArgumentParser):
    def __init__(self):
        desc = "Parses a given input and inserts into ElasticSearch."
        usage_string = "elastic-benchmark [-t/--type]"

        super(ArgumentParser, self).__init__(
            usage=usage_string, description=desc)

        self.prog = "Argument Parser"

        self.add_argument(
            "-t", "--type", metavar="<benchmark type>",
            required=True, default=None, choices=['pkb', 'tempest', 'rally'],
            help="The benchmarking tool used (ex. pkb, tempest, rally).")

        self.add_argument('input', nargs='?', type=argparse.FileType('r'),
                          default=sys.stdin)


def entry_point():
    cl_args = ArgumentParser().parse_args()
    func = globals()["parse_{0}_output".format(cl_args.type)]
    output = func(cl_args.input.read())
    esc = ElasticSearchClient()
    esc.index(run_type=cl_args.type, **output)
