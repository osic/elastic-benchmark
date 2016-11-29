import argparse
import collections
import io
import re
import sys
import json
import subunit
import testtools

from datetime import datetime
from pathlib import Path

from elastic_benchmark.main import ElasticSearchClient


# Currently Unused
def parse_console_output(output):
    deploy_pat = "(?P<name>.*)\s*: ok=(?P<ok>[0-9]*)\s+changed=(?P<changed>[0-9]*)\s+unreachable=(?P<unreachable>[0-9]*)\s+failed=(?P<failed>[0-9]*)\s+Run Time = (?P<runtime>[0-9]*)"
    upgrade_pat = "(?P<name>.*)\s*: ok=(?P<ok>[0-9]*)\s+changed=(?P<changed>[0-9]*)\s+unreachable=(?P<unreachable>[0-9]*)\s+failed=(?P<failed>[0-9]*)"
    data = open("upgradeOSASetup.txt").read()
    runs = []
    errors = 0
    ok_total = 0
    changed_total = 0

    for match in re.finditer(upgrade_pat, data):
        dic = match.groupdict()
        dic['name'] = dic['name'].strip()
        runs.append(dic)

    errors = sum([run.get("unreachable") + run.get("failed") for run in runs])
    ok_total = sum([run.get("ok") for run in runs])
    changed_total = sum([run.get("changed") for run in runs])
    names = ", ".join([run.get("name") for run in runs])

    for run in runs:
        errors += run.get("unreachable") + run.get("failed")
        ok_total += run.get("ok")
        changed_total = run.get("changed")


def parse_differences(before, after):
    # If the test fails there will be no after tests so it will skip differences logic
    if before == None:
        return {"smoke_before_success_pct": None,
                "smoke_before_success_total": None,
                "smoke_before_failures_total": None}

    if after:
        different_keys = set(after.tests.keys()) - set(before.tests.keys())
        different_keys.update(set(before.tests.keys()) - set(after.tests.keys()))
        different_keys.update([key for key, value in after.tests.items()
                           if before.tests.get(key) != value])
   
        before_percentage = int((before.success / float(before.total)) * 100)
        after_percentage = int((after.success / float(after.total))  * 100)

        return {"smoke_different_tests": ", ".join(different_keys),
                "smoke_before_success_pct": before_percentage,
                "smoke_after_success_pct": after_percentage,
                "smoke_before_success_total": before.success,
                "smoke_after_success_total": after.success,
                "smoke_before_failures_total": before.failure + before.error,
                "smoke_after_failures_total": after.failure + after.error}
    else:
        before_percentage = before.success / before.total

        return {"smoke_before_success_pct": before_percentage,
                "smoke_before_success_total": before.success,
                "smoke_before_failures_total": before.failure + before.error}


def parse_persistence_validation(before, after):
    different_keys = set(after.tests.keys()) - set(before.tests.keys())
    different_keys.update(set(before.tests.keys()) - set(after.tests.keys()))
    different_keys.update([key for key, value in after.tests.items()
                           if before.tests.get(key) != value])

    before_percentage = before.success / before.total
    after_percentage = after.success / after.total

    return {"pers_different_tests": ", ".join(different_keys),
            "pers_before_success_pct": before_percentage,
            "pers_after_success_pct": after_percentage,
            "pers_before_success_total": before.success,
            "pers_after_success_total": after.success,
            "pers_before_failures_total": before.failure + before.error,
            "pers_after_failures_total": after.failure + after.error}


def parse_uptime(output):
    # This is for cases when test fails soon
    if output == None:
        return {"api_uptime": None}
    data = json.loads(open(output).read())
    api_data = {}

    for k,v in data.items():
        api_data.update({"{0}_api_uptime".format(k): v["uptime_pct"]})
        api_data.update({"{0}_api_success".format(k): v["successful_requests"]})
        api_data.update({"{0}_api_total".format(k): v["total_requests"]})

    return api_data


def parse_during(output):
    # This is for cases when test fails soon
    if output == None:
        return {"during_uptime": None}
                                                                                             
    data = json.loads(open(output).read())
    during_data = {}

    for k,v in data.items():
        during_data.update({"{0}_during_uptime".format(k): v["uptime_pct"]})
        during_data.update({"{0}_during_success".format(k): v["successful_requests"]})
        during_data.update({"{0}_during_total".format(k): v["total_requests"]})
	during_data.update({"{0}_during_total".format(k): v["down_time"]})
	
    return during_data


def parse_persistence(output):
    # This is for cases when test fails soon
    if output == None:
        return {"persistence_uptime": None}

    data = json.loads(open(output).read())
    body = {}

    for k,v in data.items():
        for s in v['create']:
            body.update({k + '_' + s['task']: s['create']})
        for s in v['after-validate']:
            body.update({k + '_' + s['task']: s['after-validate']})
        for s in v['before-validate']:
            body.update({k + '_' + s['task']: s['before-validate']})
        for s in v['cleanup']:
            body.update({k + '_' + s['task']: s['cleanup']})
    return body


class SubunitParser(testtools.TestResult):
    def __init__(self):
        super(SubunitParser, self).__init__()
        self.tests = {}
        self.success = 0
        self.skip = 0
        self.error = 0
        self.failure = 0
        self.total = 0

    def addSuccess(self, test, details=None):
        output = test.shortDescription() or test.id()
        self.success += 1
        self.total += 1
        self.tests.update({output: "success"})

    def addSkip(self, test, err, details=None):
        output = test.shortDescription() or test.id()
        self.skip += 1
        self.tests.update({output: "skip"})

    def addError(self, test, err, details=None):
        output = test.shortDescription() or test.id()
        self.error += 1
        self.total += 1
        self.tests.update({output: "error"})

    def addFailure(self, test, err, details=None):
        output = test.shortDescription() or test.id()
        self.failure += 1
        self.total += 1
        self.tests.update({output: "failure"})

    def stopTestRun(self):
        super(SubunitParser, self).stopTestRun()

    def startTestRun(self):
        super(SubunitParser, self).startTestRun()


class FileAccumulator(testtools.StreamResult):

    def __init__(self, non_subunit_name='pythonlogging'):
        super(FileAccumulator, self).__init__()
        self.route_codes = collections.defaultdict(io.BytesIO)
        self.non_subunit_name = non_subunit_name

    def status(self, **kwargs):
        if kwargs.get('file_name') != self.non_subunit_name:
            return
        file_bytes = kwargs.get('file_bytes')
        if not file_bytes:
            return
        route_code = kwargs.get('route_code')
        stream = self.route_codes[route_code]
        stream.write(file_bytes)


class ArgumentParser(argparse.ArgumentParser):
    def __init__(self):
        desc = "Parses data from an upgrade and inserts into ElasticSearch."
        usage_string = "elastic-upgrade [-b/--before] [-a/--after] [-c/--console] [-l/--logs] [-u/--uptime]"

        super(ArgumentParser, self).__init__(
            usage=usage_string, description=desc)

        self.prog = "Argument Parser"
	
        group = self.add_mutually_exclusive_group()
        group.add_argument(
            "-b", "--before", metavar="<before subunit>",
            required=False, default=None, help="A link to the subunit from the run before the upgrade.")
        group.add_argument(
            "-s", "--status", metavar="<status log>",
            required=False, default=None, help="status updated each scenario of during")

        self.add_argument(
            "-a", "--after", metavar="<after subunit>",
            required=False, default=None, help="A link to the subunit from the run after the upgrade.")
                                                                                                                         
        self.add_argument(
            "-c", "--console", metavar="<console output>",
            required=False, default=None, help="A link to the console output from the upgrade.")

        self.add_argument(
            "-u", "--uptime", metavar="<uptime output>",
            required=False, default=None, help="A link to the uptime output from the upgrade.")

        self.add_argument(
            "-d", "--during", metavar="<during output>",
            required=False, default=None, help="A link to the during output from the upgrade.")

        self.add_argument(
            "-p", "--persistence", metavar="<persistence test output>",
            required=False, default=None, help="A link to the persistence test output from the upgrade.")

        self.add_argument(
            "-e", "--pre", metavar="<persistence test pre val output>",
            required=False, default=None, help="A link to the pre val persistence test output from the upgrade.")

        self.add_argument(
            "-o", "--post", metavar="<persistence test post val output>",
            required=False, default=None, help="A link to the post val persistence test output from the upgrade.")

        self.add_argument(
            "-l", "--logs", metavar="<log link>",
            required=False, default=None, help="A link to the logs.")

        self.add_argument('input', nargs='?', type=argparse.FileType('r'),
                          default=sys.stdin)


def parse(subunit_file, non_subunit_name="pythonlogging"):
    # In some cases the upgrade may fail in the before test section and there will be no after
    if subunit_file == None:
        return None

    subunit_parser = SubunitParser()
    stream = open(subunit_file, 'rb')
    suite = subunit.ByteStreamToStreamResult(
      stream, non_subunit_name=non_subunit_name)
    result = testtools.StreamToExtendedDecorator(subunit_parser)
    accumulator = FileAccumulator(non_subunit_name)
    result = testtools.StreamResultRouter(result)
    result.add_rule(accumulator, 'test_id', test_id=None)
    result.startTestRun()
    suite.run(result)

    for bytes_io in accumulator.route_codes.values():  # v1 processing
        bytes_io.seek(0)
        suite = subunit.ProtocolTestCase(bytes_io)
        suite.run(subunit_parser)
    result.stopTestRun()

    return subunit_parser


def entry_point():
    current_time = ''
    differences = {}
    cl_args = ArgumentParser().parse_args()
    esc = ElasticSearchClient()

    # Parses aggregate log file
    if cl_args.status == None:
        current_time = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"))
	
	open('/home/ubuntu/output/date.json','w')
        f = open('/home/ubuntu/output/date.json','a')
        f.write(json.dumps(current_time))
        f.close()
	
	print "Start aggregating results."
	if cl_args.before:
            before = parse(cl_args.before)
            after = parse(cl_args.after)
            differences = parse_differences(before, after)
        differences.update(parse_uptime(cl_args.uptime))
        differences.update(parse_during(cl_args.during))
        differences.update(parse_persistence(cl_args.persistence))
        differences.update({"done_time": current_time})
	print differences
        esc.index(scenario_name='upgrade_test', env='osa_onmetal', **differences)
	print "Done aggregating results. "
    else:
	status_files = [status_files.strip() for status_files in (cl_args.status).split(",")]

	for s in status_files:
            # Parses status log file
	    print "Start parsing status file: " + cl_args.status
            my_file = Path('/home/ubuntu/output/date.json')

            if my_file.is_file():
            # file exists
	        with open('/home/ubuntu/output/date.json') as f:
                    for line in f:
		        current_time = json.loads(line)
	    else:
		current_time = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"))

	    with open(s) as f:
		for line in f:
		    if line.strip():
	                line = json.loads(line)
			line.update({"done_time": current_time})
			esc.index(scenario_name='upgrade_status_log_test', env='osa_onmetal', **line)
	    print "Done parsing " + str(s)
