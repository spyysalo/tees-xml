#!/usr/bin/env python

from __future__ import print_function

import sys
import xml.etree.ElementTree as ET

from itertools import chain
from collections import defaultdict
from logging import info, warn, error


class FormatError(Exception):
    pass


class Document(object):
    def __init__(self, id_, orig_id, text, sentences):
        self.id = id_
        self.orig_id = orig_id
        self.text = text
        self.sentences = sentences
        for s in self.sentences:
            s.document = self
        self.assign_uids()

    def assign_uids(self, next_free_idx=None):
        """Generate document-level unique IDs replacing non-unique TEES IDs."""
        if next_free_idx is None:
            next_free_idx = defaultdict(lambda: 1)            
        for s in self.sentences:
            s.assign_uids(next_free_idx)
        
    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        orig_id = element.attrib['origId']
        text = element.attrib['text']
        sentences = []
        for sentence in element.findall('sentence'):
            try:
                sentences.append(Sentence.from_xml(sentence))
            except Exception as e:
                raise FormatError('in document {}'.format(id_)) from e
        return cls(id_, orig_id, text, sentences)


class Sentence(object):
    def __init__(self, id_, text, offset, entities, tokens, phrases,
                 dependencies):
        self.id = id_
        self.text = text
        self.start, self.end = map(int, offset.split('-'))
        assert self.start <= self.end
        self.offset = offset
        self.entities = entities
        self.tokens = tokens
        self.phrases = phrases
        self.dependencies = dependencies
        for i in chain(self.tokens, self.phrases, self.entities,
                       self.dependencies):
            i.sentence = self
        self.token_by_id = { t.id: t for t in self.tokens }
        self.document = None

    def assign_uids(self, next_free_idx):
        for i in chain(self.tokens, self.phrases, self.entities,
                       self.dependencies):
            i.assign_uid(next_free_idx)

    def get_token_uid(self, id_):
        return self.token_by_id.get(id_).uid

    def find_head(self, start, end):
        """Return Token most likely to be head of given span."""
        spanned = [
            t for t in self.tokens
            if t.start < end and t.end > start
        ]
        max_score = max(t.head_score for t in spanned)
        max_scoring = [t for t in spanned if t.head_score == max_score]
        if len(max_scoring) > 1:
            info('multiple tokens with max head_score, '+
                 'arbitrarily choosing last: {}'.format(max_scoring))
        return max_scoring[-1]
            
    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        text = element.attrib['text']
        offset = element.attrib['charOffset']
        entities, tokens, phrases, dependencies = [], [], [], []
        for entity in element.findall('evex_entity'):
            try:
                entities.append(Entity.from_xml(entity))
            except Exception as e:
                raise FormatError('in sentence {}'.format(id_)) from e
        for analyses in element.findall('analyses'):
            for tokenization in analyses.findall('tokenization'):
                for token in tokenization.findall('token'):
                    try:
                        tokens.append(Token.from_xml(token))
                    except Exception as e:
                        raise FormatError('in sentence {}'.format(id_)) from e
            for parse in analyses.findall('parse'):
                for dependency in parse.findall('dependency'):
                    try:
                        dependencies.append(Dependency.from_xml(dependency))
                    except Exception as e:
                        raise FormatError('in sentence {}'.format(id_)) from e
                for phrase in parse.findall('phrase'):
                    try:
                        phrases.append(Phrase.from_xml(phrase))
                    except Exception as e:
                        raise FormatError('in sentence {}'.format(id_)) from e
        return cls(id_, text, offset, entities, tokens, phrases, dependencies)


class Span(object):
    def __init__(self, offset):
        self.start, self.end = map(int, offset.split('-'))
        assert self.start <= self.end
        self.offset = offset
        self.type = None
        self.sentence = None
        self.uid = None

    def assign_uid(self, next_free_idx):
        self.uid = 'T{}'.format(next_free_idx['T'])
        next_free_idx['T'] += 1

    def retype(self, type_map):
        self.type = type_map.get(self.type, self.type)

    def to_ann(self, base_offset=0):
        start = self.start + base_offset
        end = self.end + base_offset
        return '{}\t{} {} {}\t{}'.format(self.uid, self.type, start, end,
                                         self.text)


class Entity(Span):
    def __init__(self, id_, type_, offset, text, orig_id):
        super(Entity, self).__init__(offset)
        self.id = id_
        self.type = type_
        self.text = text
        self.orig_id = orig_id

    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        type_ = element.attrib['entity_type']
        offset = element.attrib['charOffset']
        text = element.attrib['text']
        orig_id = element.attrib['origId']
        return cls(id_, type_, offset, text, orig_id)


class Token(Span):
    def __init__(self, id_, pos, offset, text, head_score):
        super(Token, self).__init__(offset)
        self.id = id_
        self.pos = pos
        self.text = text
        self.head_score = head_score
        self.type = 'Token'

    def __str__(self):
        return '{}/{}'.format(self.text, self.pos)

    def __repr__(self):
        return 'Token({}, {}, {}, {}, {})'.format(
            self.id, self.pos, self.offset, self.text, self.head_score)

    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        pos = element.attrib['POS']
        offset = element.attrib['charOffset']
        text = element.attrib['text']
        head_score = element.get('headScore', -99)    # TODO magic number
        return cls(id_, pos, offset, text, head_score)


class Dependency(object):
    def __init__(self, id_, type_, start, end):
        self.id = id_
        self.type = type_
        self.start = start
        self.end = end
        self.sentence = None
        self.uid = None

    def assign_uid(self, next_free_idx):
        self.uid = 'R{}'.format(next_free_idx['R'])
        next_free_idx['R'] += 1

    def to_ann(self):
        s_id = self.sentence.get_token_uid(self.start)
        e_id = self.sentence.get_token_uid(self.end)
        return '{}\t{} Arg1:{} Arg2:{}'.format(self.uid, self.type, s_id, e_id)

    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        start = element.attrib['t1']
        end = element.attrib['t2']
        type_ = element.attrib['type']
        return cls(id_, type_, start, end)


class Phrase(Span):
    def __init__(self, id_, type_, offset):
        super(Phrase, self).__init__(offset)
        self.id = id_
        self.type = type_

    @classmethod
    def from_xml(cls, element):
        id_ = element.attrib['id']
        type_ = element.attrib['type']
        offset = element.attrib['charOffset']
        return cls(id_, type_, offset)
