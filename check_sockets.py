#!/usr/bin/env python
import argparse
import json
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


class Checker(object):

    def __init__(self, args):
        self.enable_performance_data = args.enable_performance_data

        self.perf_data = list()
        self.data = list()
        self.summary = list()
        self.filter = filter

        self.check_status = 'OK'

    def __add_performance_data(self, stats):
        for k,v in stats.items():
            self.perf_data.insert(0, ('%s=%s;;;;' % (k, v)))

    def __calculate_performance_data(self, output):
        return dict(
            number_of_open_socket=len(output.split('\n')),
            number_of_waiting_sockets=len([x for x in output.split('\n') if 'TIME_WAIT' in x]),
        )

    def check(self):
        cmd = 'netstat -an --protocol=inet,inet6 | egrep "tcp|tcp6|udp|udp6"'
        if filter:
            cmd += "| grep %s" % filter

        output, errors = execute_command(cmd)

        self.summary.append(output)

        self.__add_performance_data(self.__calculate_performance_data(output))

        self.__set_status()
        self.__nagios_output()

    def __set_status(self):
        self.check_status = 'OK'

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

    args = parser.parse_args()

    check = Checker(args)
    check.check()

