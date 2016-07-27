import argparse
import datetime
import dateutil.parser
import json
import re
import sys
import uuid

from elasticsearch import Elasticsearch


class ElasticSearchClient(object):
    def __init__(self):
        self.client = Elasticsearch()

    def index(self, scenario_name, run_id, run_at, runtime, result, **kwargs):
        kwargs.update({
            "run_id": run_id,
            "run_at": run_at,
            "runtime": runtime,
            "result": result})
        self.client.index(
            index=scenario_name.lower()+"_new_schema", doc_type='results', body=kwargs)


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
    timestamp = [o.get("timestamp") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    total_runtime = [o.get("value") for o in json_outputs
                   if o.get("metric") == "End to End Runtime"][0]
    return_data = []
    run_id = str(uuid.uuid4())
    for i in xrange(num_servers):
        return_data.append({
            "scenario_name": "create_server_pkb",
            "run_id": run_id,
            "run_at": datetime.datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%S%z"),
            "runtime": total_runtime / num_servers,
            "result": "pass"})
    return return_data


def parse_tempest_output(output):
    csv_results = output.splitlines()[1:]

    results = []
    for row in csv_results:
        irow = row.split(',')

        # Skip the row if it's skipped
        if irow[1] == 'skip':
            continue

        # Ignore the test id, take only the test method name
        scenario_name = irow[0].split('[')[0].split('.')[-2]
        action_name = irow[0].split('[')[0].split('.')[-1]
        start_time = dateutil.parser.parse(irow[2])
        run_at = start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        stop_time = dateutil.parser.parse(irow[3])
        run_time = (stop_time - start_time).seconds

        run_id = [r for r in results if r.get('scenario_name') == scenario_name]
        run_id = run_id[0].get("run_id") if len(run_id) > 0 else str(uuid.uuid4())

        results.append({
            "scenario_name": scenario_name,
            "action_name": action_name,
            "run_id": run_id,
            "run_at": run_at,
            "runtime": run_time,
            "result": "pass" if irow[1] == "success" else "fail"})
    return results


def parse_rally_output(output):
    json_output = json.loads(output)
    return_data = []
    for o in json_output:
        scenario_name = o.get("key").get("name")
        run_id = str(uuid.uuid4())
        for ir in o.get('result'):
            run_at = ir.get('timestamp')
            duration = ir.get('duration')
            result = 'pass' if len(ir.get('error')) == 0 else 'fail'
            return_data.append({
                "scenario_name": scenario_name,
                "run_id": run_id,
                "run_at": datetime.datetime.fromtimestamp(int(run_at)).strftime("%Y-%m-%dT%H:%M:%S%z"),
                "runtime": duration,
                "result": result})
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

        self.add_argument(
            "-l", "--logs", metavar="<log link>",
            required=False, default=None, help="A link to the logs.")

        self.add_argument('input', nargs='?', type=argparse.FileType('r'),
                          default=sys.stdin)


def entry_point():
    cl_args = ArgumentParser().parse_args()
    func = globals()["parse_{0}_output".format(cl_args.type)]
    output = func(cl_args.input.read())
    esc = ElasticSearchClient()
    for line in output:
        esc.index(run_type=cl_args.type, logs=cl_args.logs, **line)
