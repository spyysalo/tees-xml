#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import gzip
import xml.etree.ElementTree as ET

from logging import warn, error

from teesxml import Document, Sentence, Entity, Token, Dependency


DEFAULT_OUTPUT='converted'


def argparser():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('files', metavar='FILE', nargs='+',
                    help='Input TEES XML files')
    ap.add_argument('-i', '--ids', metavar='ID[,ID ...]', default=None,
                    help='Only output documents with given IDs')
    ap.add_argument('-l', '--limit', default=100, type=int,
                    help='Maximum number of documents to process')
    ap.add_argument('-o', '--output-dir', default=DEFAULT_OUTPUT,
                    help='Output directory (default {})'.format(DEFAULT_OUTPUT))
    ap.add_argument('-s', '--sentences', default=False, action='store_true',
                    help='Output one sentence per file')
    ap.add_argument('-p', '--pairs-only', default=False, action='store_true',
                    help='Only output sentences with entity pairs')
    ap.add_argument('-d', '--no-deps', default=False, action='store_true',
                    help='Do not output dependencies')
    ap.add_argument('-t', '--no-tokens', default=False, action='store_true',
                    help='Do not output tokens (implies --no-deps)')
    return ap


def process_sentence(sentence, fn, options):
    s = Sentence.from_xml(sentence)
    if options.pairs_only and len(s.entities) < 2:
        return 0
    txt_fn = os.path.join(options.output_dir, s.id + '.txt')
    ann_fn = os.path.join(options.output_dir, s.id + '.ann')
    with open(txt_fn, 'w') as f:
        print(s.text, file=f)
    with open(ann_fn, 'w') as f:
        for e in s.entities:
            print(e.to_ann(), file=f)
        if not options.no_tokens:
            for t in s.tokens:
                print(t.to_ann(), file=f)
            if not options.no_deps:
                for d in s.dependencies:
                    print(d.to_ann(), file=f)
    return 1


def write_annotations(sentence, out, base_offset, options):
    for e in sentence.entities:
        print(e.to_ann(base_offset), file=out)
    if not options.no_tokens:
        for t in sentence.tokens:
            print(t.to_ann(base_offset), file=out)
        if not options.no_deps:
            for d in sentence.dependencies:
                print(d.to_ann(), file=out)


def write_sentence(sentence, id_base, fn, options):
    txt_fn = os.path.join(options.output_dir, id_base + '-' + sentence.id + '.txt')
    ann_fn = os.path.join(options.output_dir, id_base + '-' + sentence.id + '.ann')
    with open(txt_fn, 'w') as out:
        print(sentence.text, file=out)
    with open(ann_fn, 'w') as out:
        write_annotations(sentence, out, 0, options)


def write_document(document, fn, options):
    if options.sentences:
        for s in document.sentences:
            write_sentence(s, document.orig_id, fn, options)
    else:
        txt_fn = os.path.join(options.output_dir, document.orig_id + '.txt')
        ann_fn = os.path.join(options.output_dir, document.orig_id + '.ann')
        with open(txt_fn, 'w') as out:
            print(document.text, file=out)
        with open(ann_fn, 'w') as out:
            for s in document.sentences:
                write_annotations(s, out, s.start, options)


def read_document(element, fn, options):
    return Document.from_xml(element)


def process_stream(stream, fn, options):
    count = 0
    for event, element in stream:
        if count >= options.limit:
            break
        if event == 'end' and element.tag == 'document':
            if options.ids is not None:
                if element.attrib.get('origId') not in options.ids:
                    continue
            document = read_document(element, fn, options)
            write_document(document, fn, options)
            element.clear()
            count += 1
        else:
            pass    # TODO others?


def process(fn, options):
    if not fn.endswith('.gz'):
        process_stream(ET.iterparse(fn), fn, options)
    else:
        with gzip.GzipFile(fn) as stream:
            process_stream(ET.iterparse(stream), fn, options)


def main(argv):
    args = argparser().parse_args(argv[1:])
    if args.ids is not None:
        args.ids = args.ids.split(',')
    for fn in args.files:
        process(fn, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
