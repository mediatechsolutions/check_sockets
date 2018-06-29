#!/usr/bin/env python
import argparse
import json
import re
import sys
import subprocess

nagios_output_state = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}


def execute_command(cmd):
    p = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.communicate()


class Kubernetes(object):
    def __init__(self, kubeconfig):
        self.kubeconfig = kubeconfig

    def get_all_pod_names(self):
        cmd = "kubectl --kubeconfig %s get pods --field-selector=status.phase=Running -o custom-columns=:metadata.name" % self.kubeconfig
        output, errors = execute_command(cmd)
        return output.split('\n')

    def execute_command_on_all_pods(self, cmd):
        result = dict()

        for pod in self.get_all_pod_names():
            if pod == '' or pod == None:
                continue
            command = "kubectl --kubeconfig %s exec -ti %s -- %s" % (self.kubeconfig, pod, cmd)
            output, errors = execute_command(command)
            result[pod] = dict(
                stdout=output,
                stderr=errors,
            )
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

    def __add_performance_data(self, stats):
        for container, stats in stats.items():
            key_name = '%s.%s' % (container, stats['protocol'])
            self.perf_data.insert(0, ('%s.open=%s;%s;%s;;' % (key_name, stats['open'], self.warning, self.critical)))
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
        result = self.kubernetes.execute_command_on_all_pods('cat /proc/net/sockstat')
        socket_data = self.__get_socket_data(result)

        #self.data.append(json.dumps(socket_data, indent=4, sort_keys=True))

        self.__calculate_status(socket_data)
        self.__add_performance_data(socket_data)
        self.__nagios_output()


    def __nagios_output(self):
        output = self.check_status

        if self.summary:
            output += '\n\n%s' % '\n'.join(self.summary)
        if self.data:
            output += '\n\n%s' % '\n'.join(self.data)
        if self.enable_performance_data:
            output += '\n\n|%s' % (' '.join(self.perf_data))

        print(output)
        sys.exit(nagios_output_state[self.check_status])


if __name__ == "__main__":
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

    args = parser.parse_args()

    check = Checker(args)
    check.check()

