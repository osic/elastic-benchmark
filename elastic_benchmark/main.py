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

    def index(self, scenario_name, env, **kwargs):
        self.client.index(
            index="{0}_{1}".format(env, scenario_name.lower()),
            doc_type='results', body=kwargs)


def parse_output(output):
    json_output = json.loads(output)
    return_data = []
    for o in json_output:
        scenario_name = o.get("key", {}).get("kw", {}).get("args", {}).get(
            "alternate_name", None) or o.get("key", {}).get("name", None)
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
                "atomic_actions": {key.replace(".", ":"): val for key, val in ir.get("atomic_actions").items()},
                "result": result})

    agg = {}
    for item in return_data:
        run_id = item.get("run_id")
        if run_id in agg.keys():
            agg.get(run_id).get("runtime").append(item.get("runtime"))
            agg.get(run_id).get("atomic_actions").append(item.get("atomic_actions"))
            result = 1 if item.get("result") == "pass" else 0
            agg.get(run_id).update({
                "passes": agg.get(run_id).get("passes") + result,
                "count": agg.get(run_id).get("count") + 1})
        else:
            agg.update({item.get("run_id"): {
                "passes": 1 if item.get("result") == "pass" else 0,
                "scenario": item.get("scenario_name"),
                "count": 1, "timestamp": item.get("run_at"),
                "runtime": [item.get("runtime")],
                "atomic_actions": [item.get("atomic_actions")]}})
    for run_id, dic in agg.items():
        atomic_actions = {k: {"min": min(v), "max": max(v), "avg": sum(v)/len(v)}
                          for k, v in {ok: [float(i.get(ok)) for i in dic.get("atomic_actions") if i.get(ok) <> None]
                          for ok in set().union(*(d.keys() for d in dic.get("atomic_actions")))}.items()}
        return_data.append({
            "scenario_name": "aggregated_results",
            "scenario": dic.get("scenario"),
            "run_id": run_id,
            "timestamp": dic.get("timestamp"),
            "success_percentage": float(dic.get("passes")) / float(dic.get("count")),
            "avg_runtime": float(sum(dic.get("runtime"))) / float(dic.get("count")),
            "action_count": dic.get("count"),
            "atomic_actions": atomic_actions})
    return return_data


class ArgumentParser(argparse.ArgumentParser):
    def __init__(self):
        desc = "Parses a given input and inserts into ElasticSearch."
        usage_string = "elastic-benchmark [-t/--type]"

        super(ArgumentParser, self).__init__(
            usage=usage_string, description=desc)

        self.prog = "Argument Parser"

        self.add_argument(
            "-e", "--environment", metavar="<environment>",
            required=True, default="devstack",
            help="The environment you're running against.")

        self.add_argument(
            "-l", "--logs", metavar="<log link>",
            required=False, default=None, help="A link to the logs.")

        self.add_argument('input', nargs='?', type=argparse.FileType('r'),
                          default=sys.stdin)


def entry_point():
    cl_args = ArgumentParser().parse_args()
    output = parse_output(cl_args.input.read())
    esc = ElasticSearchClient()
    for line in output:
        esc.index(logs=cl_args.logs, env=cl_args.environment, **line)
