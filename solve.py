#!/bin/env python
#encoding=utf-8

from __future__ import print_function

import os
import sys
import shutil
import math
import Queue

# pip install mmh3
import mmh3
# pip install pygtrie
import pygtrie as trie

def part(filename):
    split_bytes = 512 * (1024 * 1024)
    file_size_to_split = os.path.getsize(filename)
    num_to_split = int(math.ceil(1.0 * file_size_to_split / split_bytes))
    print('`{}`({}) going to split into {} parts'.format(
        filename, file_size_to_split, num_to_split
    ))

    dirname = './{}-parted'.format(filename)
    if os.path.exists(dirname) and os.path.isdir(dirname):
        shutil.rmtree(dirname)
    os.mkdir(dirname)
    outfiles_name = [
        '{}/part-{:05d}'.format(dirname, x)
        for x in range(num_to_split)
    ]
    outfiles = list(map(lambda fn: open(fn, 'wb'), outfiles_name))
    outfiles_size = [0] * num_to_split

    with open(filename, 'r') as infile:
        for line in infile:
            line_len = len(line)
            line = line.strip()
            h = mmh3.hash(line) % num_to_split
            print(line, file=outfiles[h])
            outfiles_size[h] += line_len
    map(lambda x: x.close(), outfiles)

    # TODO  check outfiles_size if thereis file larger than split_bytes
    return dirname


def count(filename, counter_type='dict'):
    if counter_type == 'dict':
        counter = {}
    elif counter_type == 'trie':
        counter = trie.CharTrie()
    else:
        assert(False, 'error counter type')
    print('counting `{}` using type: {}'.format(filename, counter_type))
    with open(filename, 'r') as infile:
        for lineno, line in enumerate(infile):
            if lineno % 50000 == 0:
                print('processing {} line'.format(lineno))
            url = line.strip()
            prev_cnt = counter.get(url, 0)
            counter[url] = prev_cnt + 1
    print('dump counting into file...')
    with open(filename + '_cnt', 'wb') as outfile:
        for k in counter:
            print('{},{}'.format(k, counter[k]), file=outfile)
    print('dump done')


class Item(object):
    def __init__(self, url, cnt):
        self.url = url
        self.cnt = cnt

    def __lt__(self, rhs):
        return self.cnt < rhs.cnt

    def __str__(self):
        return '({},{})'.format(self.url, self.cnt)

    def __repr__(self):
        return str(self)


def reduce_counters(dirname, topN=1000):
    q = Queue.PriorityQueue(topN)
    for _fn in os.listdir(dirname):
        fn = os.path.join(dirname, _fn)
        if not fn.endswith('_cnt'):
            continue
        print('itering file: `{}`'.format(fn))
        with open(fn, 'r') as infile:
            for line in infile:
                url, cnt = line.strip().split(',')
                cnt = int(cnt)
                assert(q.qsize() <= q.maxsize)
                if q.qsize() == q.maxsize:
                    # 弹出最小的元素, 对比, 压较大的元素入堆
                    cur_min = q.get()
                    if cur_min.cnt < cnt:
                        q.put(Item(url, cnt))
                    else:
                        q.put(cur_min)
                else:
                    # 未满, 入堆
                    q.put(Item(url, cnt))


if __name__ == '__main__':
    filename = sys.argv[1]
    
    parted_dir = part(filename)

    for _fn in os.listdir(parted_dir):
        fn = os.path.join(parted_dir, _fn)
        count(fn)
    
    reduce_counters(parted_dir)
