#!/usr/bin/env python3
import argparse

from initial_setup_confirmation import EclNetApp


def main():
    parser = argparse.ArgumentParser(description='NetApp initial setup confirmation script')
    parser.add_argument('ip', type=str,
                        help='IPv4 address of NetApp to be configured',
                        metavar='<ip>')

    parser.add_argument('-u','--username', type=str,
                        help='Username of NetApp',
                        default='admin',
                        metavar='<username>')

    parser.add_argument('-p','--password', type=str,
                        help='Password for user',
                        default='b3arm3tal',
                        metavar='<password>')

    args = parser.parse_args()
    ip = args.ip
    username = args.username
    password = args.password
    print('IP: %s' % (ip))
    netapp = EclNetApp(filer=ip, user=username, password=password)
    netapp.get_cluster_name()

if __name__ == '__main__':
    main()

