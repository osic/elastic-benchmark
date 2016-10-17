import argparse
import collections
import io
import re
import sys

import subunit
import testtools

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
    different_keys = set(after.keys()) - set(before.keys())
    different_keys.update(set(before.keys()) - set(after.keys()))
    different_keys.update([key for key, value in after.items()
                           if before.get(key) != value])

    before_percentage = before.success / before.total
    after_percentage = after.success / after.total

    return {"different_tests": ", ".join(different_keys),
            "before_success_pct": before_percentage,
            "after_success_pct": after_percentage,
            "before_success_total": before.success,
            "after_success_total": after.success,
            "before_failures_total": before.failure + before.error,
            "after_failures_total": after.failure + after.error}


def parse_uptime(output):
    data = json.loads(open(output).read())

    return {"{0}_uptime".format(k): v.get("uptime_pct") for k, v in data.items()}


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

        self.add_argument(
            "-b", "--before", metavar="<before subunit>",
            required=True, default=None, help="A link to the subunit from the run before the upgrade.")

        self.add_argument(
            "-a", "--after", metavar="<after subunit>",
            required=True, default=None, help="A link to the subunit from the run after the upgrade.")

        self.add_argument(
            "-c", "--console", metavar="<console output>",
            required=False, default=None, help="A link to the console output from the upgrade.")

        self.add_argument(
            "-u", "--uptime", metavar="<uptime output>",
            required=True, default=None, help="A link to the uptime output from the upgrade.")

        self.add_argument(
            "-l", "--logs", metavar="<log link>",
            required=False, default=None, help="A link to the logs.")

        self.add_argument('input', nargs='?', type=argparse.FileType('r'),
                          default=sys.stdin)


def parse(subunit_file, non_subunit_name="pythonlogging"):
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
    cl_args = ArgumentParser().parse_args()
    esc = ElasticSearchClient()
    before = parse(cl_args.before)
    after = parse(cl_args.after)
    differences = parse_differences(before, after)
    differences.update(parse_uptime(cl_args.uptime))
    esc.index(scenario_name="upgrade", **differences)
