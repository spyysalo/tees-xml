#!/usr/bin/env python

import sys
import os

from sqlitedict import SqliteDict


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser(description='List keys in SQLiteDict DB.')
    ap.add_argument('db', nargs='+')
    return ap


def list_db(dbname):
    with SqliteDict(dbname, flag='r') as db:
        for k in db:
            print(k)


def main(argv):
    args = argparser().parse_args(argv[1:])
    for dbname in args.db:
        if not os.path.exists(dbname):
            print('no such file: {}'.format(dbname), file=sys.stderr)
            continue
        list_db(dbname)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
