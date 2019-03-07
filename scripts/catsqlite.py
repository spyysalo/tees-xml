#!/usr/bin/env python

import sys

from sqlitedict import SqliteDict


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser(description='List values in SQLiteDict DB.')
    ap.add_argument('-k', '--showkeys', default=False, action='store_true',
                    help='include keys in output')
    ap.add_argument('db', metavar='DB', help='database file')
    ap.add_argument('keys', metavar='KEY', nargs='*', help='keys to look up')
    return ap


def list_db(dbname, options):
    with SqliteDict(dbname) as db:
        if not options.keys:
            for k, v in db.iteritems():
                if options.showkeys:
                    print('==> {} <=='.format(k))
                print(v)
        else:
            for k in options.keys:
                if options.showkeys:
                    print('==> {} <=='.format(k))
                print(db[k])


def main(argv):
    args = argparser().parse_args(argv[1:])
    list_db(args.db, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
