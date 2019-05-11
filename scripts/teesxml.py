#!/usr/bin/env python

import sys
import xml.etree.ElementTree as ET

from itertools import chain
from collections import defaultdict, Counter
from logging import info, warning, error


# Normalization attribute constants
NORM_ATTR_PREFIX = 'norm_'
CONF_ATTR_SUFFIX = '_conf'


def _generate_unique(prefix):
    """Return unique string with given prefix."""
    _generate_unique.cache[prefix] += 1
    return '{}{}'.format(prefix, _generate_unique.cache[prefix])
_generate_unique.cache = Counter()


def get_attrib(element, key, options):
    try:
        return element.attrib[key]
    except KeyError:
        if not getattr(options, 'recover', False):
            error('missing {} in {} (consider --recover)'\
                  .format(key, element.tag))
            raise
        else:
            val = _generate_unique('MISSING.')
            warning('recover: replace missing {} in {} with {}'.\
                    format(key, element.tag, val))
            return val


def get_norm_curie(norm_type, norm_id):
    """Return CURIE form for EVEX normalization type and id."""
    # Prefixes from https://prefixcommons.org when available
    if norm_type == 'ncbitax_id':
        return 'NCBITaxon:{}'.format(norm_id)
    elif norm_type == 'entrezgene_id':
        return 'ncbigene:{}'.format(norm_id)
    elif norm_type == 'cellline_acc':
        return 'cellosaurus:{}'.format(norm_id)
    elif norm_type == 'cui' and norm_id.startswith('CHEBI:'):
        return norm_id
    elif norm_type == 'cui':
        return 'mesh:{}'.format(norm_id)
    else:
        warning('unknown norm type {}'.format(norm_type))
        return '{}:{}'.format(norm_type, norm_id)


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
    def from_xml(cls, element, options=None):
        recover = getattr(options, 'recover', False)
        id_ = element.attrib['id']
        orig_id = element.attrib['origId']
        text = element.attrib['text']
        sentences = []
        for sentence in element.findall('sentence'):
            try:
                sentences.append(Sentence.from_xml(sentence, options))
            except Exception as e:
                if recover:
                    sid = sentence.attrib.get('id')
                    error('failed to parse sentence {} in {}, ignoring'.\
                          format(sid, id_))
                else:
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
            i.assign_uids(next_free_idx)

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
    def from_xml(cls, element, options=None):
        recover = getattr(options, 'recover', False)
        id_ = element.attrib['id']
        text = element.attrib['text']
        offset = element.attrib['charOffset']
        entities = []
        for entity in element.findall('evex_entity'):
            try:
                entities.append(Entity.from_xml(entity, options))
            except Exception as e:
                if recover:
                    eid = entity.attrib.get('id')
                    etype = entity.attrib.get('entity_type')
                    error('failed to parse "{}" entity ID {} in {}, ignoring'.\
                          format(etype, eid, id_))
                else:
                    raise FormatError('in sentence {}'.format(id_)) from e

        no_tokens = getattr(options, 'no_tokens', False)
        no_phrases = not getattr(options, 'phrases', False)

        tokens, phrases, dependencies = [], [], []
        for analyses in element.findall('analyses'):
            if not no_tokens:
                for tokenization in analyses.findall('tokenization'):
                    for token in tokenization.findall('token'):
                        try:
                            tokens.append(Token.from_xml(token, options))
                        except Exception as e:
                            raise FormatError('in sentence {}'.format(id_)) from e
            for parse in analyses.findall('parse'):
                if not no_tokens:    # no tokens implies no dependencies
                    for dependency in parse.findall('dependency'):
                        try:
                            dependencies.append(Dependency.from_xml(dependency,
                                                                    options))
                        except Exception as e:
                            raise FormatError('in sentence {}'.format(id_)) from e
                if not no_phrases:
                    for phrase in parse.findall('phrase'):
                        try:
                            p = Phrase.from_xml(phrase, options)
                            p.assign_text(text)
                            phrases.append(p)
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

    def assign_uids(self, next_free_idx):
        self.uid = 'T{}'.format(next_free_idx['T'])
        next_free_idx['T'] += 1

    def retype(self, type_map):
        self.type = type_map.get(self.type, self.type)

    def to_ann_lines(self, base_offset=0):
        start = self.start + base_offset
        end = self.end + base_offset
        yield '{}\t{} {} {}\t{}'.format(self.uid, self.type, start, end,
                                        self.text)


class Entity(Span):
    def __init__(self, id_, type_, offset, text, orig_id, norm_id, norm_conf):
        super(Entity, self).__init__(offset)
        self.id = id_
        self.type = type_
        self.text = text
        self.orig_id = orig_id
        self.norm_id = norm_id
        self.norm_conf = norm_conf
        self.norm_uid = None

    def assign_uids(self, next_free_idx):
        super().assign_uids(next_free_idx)
        if self.norm_id is not None:
            self.norm_uid = 'N{}'.format(next_free_idx['N'])
            next_free_idx['N'] += 1

    def to_ann_lines(self, base_offset=0):
        start = self.start + base_offset
        end = self.end + base_offset
        yield '{}\t{} {} {}\t{}'.format(self.uid, self.type, start, end,
                                        self.text)
        if self.norm_id is not None:
            yield '{}\tReference {} {}\t{} [confidence:{}]'.format(
                self.norm_uid, self.uid, self.norm_id, self.text,
                self.norm_conf)

    @classmethod
    def from_xml(cls, element, options=None):
        id_ = get_attrib(element, 'id', options)
        type_ = element.attrib['entity_type']
        offset = element.attrib['charOffset']
        text = element.attrib['text']
        orig_id = get_attrib(element, 'origId', options)
        norm_id, norm_conf = Entity.get_normalization(element)
        return cls(id_, type_, offset, text, orig_id, norm_id, norm_conf)

    @staticmethod
    def get_normalization(element):
        norms, confs = {}, {}
        # gather normalization and confidence attributes
        for k, v in element.attrib.items():
            if not k.startswith(NORM_ATTR_PREFIX):
                continue
            k = k[len(NORM_ATTR_PREFIX):]
            if not k.endswith(CONF_ATTR_SUFFIX):
                norms[k] = v    # normalization
            else:
                k = k[:-len(CONF_ATTR_SUFFIX)]
                confs[k] = v    # confidence
        # pair up norm and conf values
        norm_confs = []
        for k in set(norms.keys()) | set(confs.keys()):
            if k not in confs:
                warning('norm_{} without _conf, ignoring'.format(k))
            elif k not in norms:
                warning('norm_{}_conf without norm_, ignoring'.format(k))
            elif ((k == 'entrezgene_id' and norms[k] == '0') or
                  (k == 'cui' and norms[k] == 'NA')):    # interpret as empty
                info('skipping norm {}:{}'.format(k, norms[k]))
            else:
                norm_confs.append((k, norms[k], confs[k]))
        if not norm_confs:
            return None, None
        else:
            if len(norm_confs) > 1:
                warning('more than one norm, only using first: {}'.\
                        format(norm_confs))
            norm_type, norm_id, norm_conf = norm_confs[0]
            norm_curie = get_norm_curie(norm_type, norm_id)
            return norm_curie, norm_conf


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
    def from_xml(cls, element, options=None):
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

    def assign_uids(self, next_free_idx):
        self.uid = 'R{}'.format(next_free_idx['R'])
        next_free_idx['R'] += 1

    def to_ann_lines(self):
        s_id = self.sentence.get_token_uid(self.start)
        e_id = self.sentence.get_token_uid(self.end)
        yield '{}\t{} Arg1:{} Arg2:{}'.format(self.uid, self.type, s_id, e_id)

    @classmethod
    def from_xml(cls, element, options=None):
        id_ = element.attrib['id']
        start = element.attrib['t1']
        end = element.attrib['t2']
        type_ = element.attrib['type']
        return cls(id_, type_, start, end)


class Phrase(Span):
    def __init__(self, id_, type_, offset):
        super(Phrase, self).__init__(offset)
        self.id = id_
        self.type = 'Phrase-{}'.format(type_)
        self.text = None    # need assign_text

    def assign_text(self, sentence_text):
        self.text = sentence_text[self.start:self.end]

    @classmethod
    def from_xml(cls, element, options=None):
        id_ = element.attrib['id']
        type_ = element.attrib['type']
        offset = element.attrib['charOffset']
        return cls(id_, type_, offset)
