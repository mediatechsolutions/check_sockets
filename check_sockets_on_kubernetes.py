#!/usr/bin/env python
import argparse
import json
import os
import re
import sys
import subprocess
import logging
import time
from multiprocessing import Pool


nagios_output_state = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}

logger = logging.getLogger()
ERROR = logging.ERROR
WARNING = logging.WARNING
INFO = logging.INFO
DETAIL = logging.INFO - 1
DEBUG = logging.DEBUG
VERBOSITIES = [ERROR, WARNING, INFO, DETAIL, DEBUG]


def configure_logging(verbosity):
    level = VERBOSITIES[min(int(verbosity), len(VERBOSITIES) -1)]
    formatter = logging.Formatter('** %(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)


def execute_command(cmd):
    p = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.communicate()


def parallel_k8s_run(args):
    kubeconfig, cmd, podname = args
    logger.debug("parallel_k8s_run(%s, %s)", cmd, podname)
    command = "kubectl --kubeconfig %s exec %s -- %s" % (kubeconfig, podname, cmd)
    logger.debug("Executing command %s", command)
    output, errors = execute_command(command)
    return (
        podname,
        dict(
            stdout=output,
            stderr=errors,
        )
    )


class Kubernetes(object):
    def __init__(self, kubeconfig):
        self.kubeconfig = kubeconfig

    def get_all_pod_names(self):
        cmd = "kubectl --kubeconfig %s get pods --field-selector=status.phase=Running -o custom-columns=:metadata.name" % self.kubeconfig
        logger.debug("Executing command %s", cmd)
        output, errors = execute_command(cmd)
        return [x.strip() for x in output.split('\n') if x.strip()]

    def execute_command_on_pod(self, cmd, podname):
        logger.debug("execute_command_on_pod(%s, %s)", cmd, podname)
        command = "kubectl --kubeconfig %s exec %s -- %s" % (self.kubeconfig, podname, cmd)
        logger.debug("Executing command %s", command)
        output, errors = execute_command(command)
        return dict(
            stdout=output,
            stderr=errors,
        )

    def execute_command_on_pod_tuple(self, t):
        return self.execute_command_on_pod(t[0], t[1])

    def execute_command_on_all_pods(self, cmd, fast=False):
        result = dict()

        podnames = self.get_all_pod_names()
        logger.debug("%s pods found", len(podnames))
        logger.debug("pods: %s", podnames)

        if fast:
            logger.debug("Fast mode enabled")
            pool = Pool()
            raw = pool.map(parallel_k8s_run, [(self.kubeconfig, cmd, pod) for pod in podnames])
            logger.debug("result generated")
            for r in raw:
                result[r[0]] = r[1]
        else:
            for pod in podnames:
                result[pod] = self.execute_command_on_pod(cmd, pod)
        return result


class Checker(object):

    def __init__(self, args):
        self.enable_performance_data = args.enable_performance_data

        self.kubernetes = Kubernetes(args.kubeconfig)
        self.warning = args.warning
        self.critical = args.critical

        self.perf_data = list()
        self.data = list()
        self.summary = list()
        self.filter = filter

        self.check_status = 'OK'
        self.fast = args.fast

    def __add_performance_data(self, stats):
        for container, stats in stats.items():
            key_name = '%s.%s' % (container, stats['protocol'])
            self.perf_data.insert(0, ('%s.open=%s;%s;%s;;\n' % (key_name, stats['open'], self.warning, self.critical)))
#            self.perf_data.insert(0, ('%s.waiting_to_close=%s;;;;' % (key_name, stats['waiting_to_close'])))

    def __calculate_performance_data(self, output):
        return dict(
            number_of_open_socket=len(output.split('\n')),
            number_of_waiting_sockets=len([x for x in output.split('\n') if 'TIME_WAIT' in x]),
        )

    def __get_socket_data(self, pod_data):
        regex = '(TCP): inuse (\d+) orphan \d+ tw (\d+)'
        p = re.compile(regex)

        result = dict()
        for pod_name, out in pod_data.items():
            match = p.search(out['stdout'])
            protocol, open, waiting_to_close = match.groups()
            result[pod_name] = dict(
                protocol=protocol,
                open=int(open),
                waiting_to_close=int(waiting_to_close),
            )

        return result

    def __calculate_status(self, socket_data):
        self.check_status = 'OK'

        for container, stats in socket_data.items():
            if stats['open'] > self.critical:
                self.check_status ='CRITICAL'
                self.summary.append('CRITICAL %s open sockets %s > %s' % (container, stats['open'], self.critical))
                continue
            if stats['open'] > self.warning:
                if self.check_status != 'CRITICAL':
                    self.check_status = 'WARNING'
                self.summary.append('WARNING %s open sockets %s > %s' % (container, stats['open'], self.warning))

    def check(self):
        result = self.kubernetes.execute_command_on_all_pods('cat /proc/net/sockstat', self.fast)
        socket_data = self.__get_socket_data(result)

        #self.data.append(json.dumps(socket_data, indent=4, sort_keys=True))

        self.__calculate_status(socket_data)
        self.__add_performance_data(socket_data)

    def nagios_output(self):
        output = self.check_status

        if self.summary:
            output += '\n\n%s' % '\n'.join(self.summary)
        if self.data:
            output += '\n\n%s' % '\n'.join(self.data)
        if self.enable_performance_data:
            output += '\n\n|%s' % (' '.join(self.perf_data))

        return output

    def nagios_exit(self):
        print(self.nagios_output())
        sys.exit(nagios_output_state[self.check_status])


class Cache(object):
    def __init__(self, default_content='Not executed', default_rc=3, cache_liveness=60):
        self.filename = "/tmp/check_sockets.json"
        self.default_content = default_content
        self.default_rc = default_rc
        self.cache_liveness = cache_liveness

    def write(self, output, rc):
        with open(self.filename, 'w+') as fd:
            json.dump(dict(output=output, rc=rc), fd)

    def read(self):
        if (
            not os.path.exists(self.filename)
            or os.stat(self.filename).st_mtime + self.cache_liveness< time.time()
        ):
            return (self.default_content, self.default_rc)
        with open(self.filename) as fd:
            data = json.load(fd)
        return (data['output'], data['rc'])
           
 
def get_args():
    parser = argparse.ArgumentParser(
        description='Return number of open sockets')

    parser.add_argument(
        '--enable-performance-data',
        help='enable output performance data',
        action='store_true',
        default=False
    )
    parser.add_argument(
        '--kubeconfig',
        default='~/.kube/config',
    )
    parser.add_argument(
        '-w', '--warning',
        default='20000',
        type=int,
    )
    parser.add_argument(
        '-c', '--critical',
        default='25000',
        type=int,
    )
    parser.add_argument(
        '-f', '--fast',
        action="store_true",
        default=False,
        help='Run in threads in order to increase speed.'
    )
    parser.add_argument(
        '-d', '--delayed',
        action="store_true",
        default=False,
        help='Returns the last value or empty and runs it in background for the next time'
    )
    parser.add_argument(
        '--cache-liveness',
        default=60 * 60,  # seconds
        help='Seconds the cache file is valid. After that time it would be not considered.'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest='verbosity',
        default=0,
        help='Increase verbosity. Several increases even more.'
    )

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    configure_logging(args.verbosity)

    check = Checker(args)

    if args.delayed:
        cache = Cache(cache_liveness=args.cache_liveness)
        pid = os.fork() 
        if pid > 0:
            output, rc = cache.read()
            print(output)
            sys.exit(rc)
            return
        
        check.check()
        output = check.nagios_output()
        rc = nagios_output_state[check.check_status]
        cache.write(output, rc)
        return

    check.check()
    check.nagios_exit()


if __name__ == "__main__":
    main()
