#!/usr/bin/env python

import sys

from sqlitedict import SqliteDict


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser(description='List values in SQLiteDict DB.')
    ap.add_argument('-n', '--listname', default=False, action='store_true')
    ap.add_argument('db', nargs='+')
    return ap


def list_db(dbname, options):
    with SqliteDict(dbname) as db:
        for k, v in db.iteritems():
            if options.listname:
                print('==> {} <=='.format(k))
            print(v)


def main(argv):
    args = argparser().parse_args(argv[1:])
    for dbname in args.db:
        list_db(dbname, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
