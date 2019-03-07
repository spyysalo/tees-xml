#!/usr/bin/env python

import os
import sys
import gzip
import errno
import xml.etree.ElementTree as ET

from contextlib import contextmanager
from abc import ABC, abstractmethod
from logging import warn, error

from teesxml import Document, Sentence, Entity, Token, Dependency


DEFAULT_OUT='converted'

# used with --retype
TYPE_MAP = {
    'cel': 'Cell',
    'che': 'Chemical',
    'dis': 'Disease',
    'ggp': 'Gene',
    'org': 'Organism',
}

def argparser():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('files', metavar='FILE', nargs='+',
                    help='Input TEES XML files')
    ap.add_argument('-i', '--ids', metavar='ID[,ID ...]', default=None,
                    help='Only output documents with given IDs')
    ap.add_argument('-l', '--limit', default=None, type=int,
                    help='Maximum number of documents to process')
    ap.add_argument('-o', '--output', default=DEFAULT_OUT,
                    help='Output dir/db (default {})'.format(DEFAULT_OUT))
    ap.add_argument('-P', '--dir-prefix', type=int, default=None,
                    help='Add subdirectory with given length document ID prefix')
    ap.add_argument('-D', '--database', default=False, action='store_true',
                    help='Output to SQLite DB (default filesystem)')
    ap.add_argument('-s', '--sentences', default=False, action='store_true',
                    help='Output one sentence per file')
    ap.add_argument('-d', '--no-deps', default=False, action='store_true',
                    help='Do not output dependencies')
    ap.add_argument('-t', '--no-tokens', default=False, action='store_true',
                    help='Do not output tokens (implies --no-deps)')
    ap.add_argument('-T', '--retype', default=False, action='store_true',
                    help='Rename types (e.g. "dis" -> "Disease")')
    return ap


def write_annotations(sentence, out, base_offset, options):
    for e in sentence.entities:
        if options.retype:
            e.retype(TYPE_MAP)
        out.write(e.to_ann(base_offset) + '\n')
    if not options.no_tokens:
        for t in sentence.tokens:
            out.write(t.to_ann(base_offset) + '\n')
        if not options.no_deps:
            for d in sentence.dependencies:
                out.write(d.to_ann() + '\n')


# https://stackoverflow.com/a/600612
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def document_path(doc_id, options):
    """Return relative path to document from base output directory."""
    if options.dir_prefix is None:
        return ''
    else:
        return doc_id[:options.dir_prefix]


class WriterBase(ABC):
    """Abstracts over filesystem and DB for output."""
    @abstractmethod
    def open(path):
        pass


class FilesystemWriter(WriterBase):
    def __init__(self, base_dir=None):
        self.base_dir = base_dir
        self.known_directories = set()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @contextmanager
    def open(self, path):
        if self.base_dir is not None and not os.path.isabs(path):
            path = os.path.join(self.base_dir, path)
        directory = os.path.dirname(path)
        if directory not in self.known_directories:
            mkdir_p(directory)
            self.known_directories.add(directory)
        f = open(path, 'w')
        try:
            yield f
        finally:
            f.close()


class SQLiteFile(object):
    """Minimal file-like object that writes into SQLite DB"""
    def __init__(self, key, db):
        self.key = key
        self.db = db
        self.data = []

    def write(self, data):
        self.data.append(data)

    def flush(self):
        self.db[self.key] = ''.join(self.data)
        self.db.commit()

    def close(self):
        self.flush()
        self.db = None


class SQLiteWriter(WriterBase):
    def __init__(self, dbname):
        self.dbname = dbname
        self.db = None

    def __enter__(self):
        try:
            import sqlitedict
        except ImportError:
            error('failed to import sqlitedict; try `pip3 install sqlitedict`')
            raise
        self.db = sqlitedict.SqliteDict(self.dbname, autocommit=True)
        return self

    def __exit__(self, *args):
        pass

    @contextmanager
    def open(self, path):
        f = SQLiteFile(path, self.db)
        try:
            yield f
        finally:
            f.close()


def write_sentence(writer, sentence, doc_id, fn, options):
    doc_path = document_path(doc_id, options)
    txt_fn = os.path.join(doc_path, doc_id + '-' + sentence.id + '.txt')
    ann_fn = os.path.join(doc_path, doc_id + '-' + sentence.id + '.ann')
    with writer.open(txt_fn) as out:
        out.write(sentence.text + '\n')
    with writer.open(ann_fn) as out:
        write_annotations(sentence, out, 0, options)


def write_document(writer, document, fn, options):
    if options.sentences:
        for s in document.sentences:
            write_sentence(writer, s, document.orig_id, fn, options)
    else:
        doc_path = document_path(document.orig_id, options)
        txt_fn = os.path.join(doc_path, document.orig_id + '.txt')
        ann_fn = os.path.join(doc_path, document.orig_id + '.ann')
        with writer.open(txt_fn) as out:
            out.write(document.text + '\n')
        with writer.open(ann_fn) as out:
            for s in document.sentences:
                write_annotations(s, out, s.start, options)


def read_document(element, fn, options):
    return Document.from_xml(element)


def process_stream(writer, stream, fn, options):
    count = 0
    for event, element in stream:
        if options.limit is not None and count >= options.limit:
            break
        if event == 'end' and element.tag == 'document':
            if options.ids is not None:
                if element.attrib.get('origId') not in options.ids:
                    continue
            document = read_document(element, fn, options)
            write_document(writer, document, fn, options)
            element.clear()
            count += 1
        else:
            pass    # TODO others?


def process(writer, fn, options):
    if not fn.endswith('.gz'):
        process_stream(writer, ET.iterparse(fn), fn, options)
    else:
        with gzip.GzipFile(fn) as stream:
            process_stream(writer, ET.iterparse(stream), fn, options)


def main(argv):
    args = argparser().parse_args(argv[1:])
    if args.ids is not None:
        args.ids = args.ids.split(',')

    if not args.database:
        Writer, name = FilesystemWriter, args.output
    else:
        Writer, name = SQLiteWriter, args.output+'.sqlite'

    with Writer(name) as writer:
        for fn in args.files:
            process(writer, fn, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
