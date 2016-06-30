import argparse
import datetime
import json
import re
import sys

from elasticsearch import Elasticsearch


class ElasticSearchClient(object):
    def __init__(self):
        self.client = Elasticsearch()

    def index(self, scenario_name, run_at, total_runtime, individual_results,
              average_action_time, average_action_success, **kwargs):
        kwargs.update({
            "run_at": run_at,
            "total_runtime": total_runtime,
            "individual_results": individual_results,
            "average_action_time": average_action_time,
            "average_action_success": average_action_success})
        self.client.index(index=scenario_name, doc_type='results', body=kwargs)


def parse_pkb_output(output):
    regex_starts = [m.start() for m in re.finditer("{'metadata'", output)]
    regex_ends = [
        regex_starts[1],
        output[regex_starts[1]:].index("} ")+1+regex_starts[1]]
    json_outputs = [
        output[regex_starts[0]:regex_ends[0]],
        output[regex_starts[1]:regex_ends[1]]]
    json_outputs = [
        json.loads(o.replace("'", '"').replace("False", "false").strip())
        for o in json_outputs]
    num_servers = [o.get("metadata").get("vm_count") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    total_runtime = [o.get("value") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    average_action_time = total_time / num_servers
    timestamp = [o.get("timestamp") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    average_action_success = 100 # pkb doesn't seem to want to report failures
    individual_results = [
        {"resource_name": "server", "action_time": total_runtime,
         "was_successful": True} for count in range(num_servers)]
    return [{"scenario_name": "create_server_pkb",
             "total_runtime": total_runtime,
             "individual_results": individual_results
             "average_action_time": average_action_time,
             "average_action_success": average_action_success,
             "run_at": str(datetime.datetime.fromtimestamp(int(timestamp)))}]

def parse_rally_output(output):
    json_outputs = json.loads(output)
    return_data = []
    for o in json_output:
        scenario_name = o.get("key").get("name")
        # direct access but should always have at least one result right?
        run_at = o.get("result")[0].get("timestamp")
        total_runtime = o.get("full_duration")
        individual_results = []
        duration_list = [r.get("duration") for r in o.get("result")]
        average_action_time = sum(duration_list) / len(duration_list)
        success_list = [True for r in o.get("result")
                        if len(r.get("error")) > 0]
        average_action_succes = sum(success_list) / len(duration_list)
        return_data.append({
            "scenario_name": scenario_name,
            "run_at": run_at,
            "total_runtime": total_runtime,
            "individual_results": individual_results,
            "average_action_time": average_action_time,
            "average_action_success": average_action_success})
    return return_data



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
    for line in output:
        esc.index(run_type=cl_args.type, **output)
