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

def part(filename, splited_mb=512):
    # 计算拆分为多少份
    split_bytes = splited_mb * (1024 * 1024)
    file_size_to_split = os.path.getsize(filename)
    num_to_split = int(math.ceil(1.0 * file_size_to_split / split_bytes))
    print('`{}`({}) going to split into {} parts, expect {:.1f}mb'.format(
        filename, file_size_to_split, num_to_split, splited_mb
    ))

    # 生成拆分的文件夹及小文件
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

    # murmurhash3 拆分为互不重叠的小文件
    with open(filename, 'r') as infile:
        for line in infile:
            line_len = len(line)
            line = line.strip()
            h = mmh3.hash(line) % num_to_split
            print(line, file=outfiles[h])
            outfiles_size[h] += line_len
    map(lambda x: x.close(), outfiles)

    # 检查 outfiles_size 过滤出比预期大的文件 split_bytes
    skew_files = filter(
        lambda (fn, sz): sz > split_bytes, 
        zip(outfiles_name, outfiles_size)
    )
    if len(skew_files) > 0:
        # TODO 对数据倾斜的部分进一步拆分
        print('warning skew files: {}', skew_files)
        pass

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
            # 更新计数
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
    return q


if __name__ == '__main__':
    # parse args
    import argparse
    parser = argparse.ArgumentParser(description='URL counters')
    parser.add_argument('--filename', required=True,
                        help='url file to process')
    parser.add_argument('--split', type=int, default=10,
                        help='split size (mb)')
    parser.add_argument('--ntop', type=int, default=100,
                        help='num of top elements to get')
    args = parser.parse_args()

    ## main ##
    parted_dir = part(args.filename, args.split)

    for _fn in os.listdir(parted_dir):
        fn = os.path.join(parted_dir, _fn)
        count(fn)
    
    q = reduce_counters(parted_dir, args.ntop)

    ## print results ##
    # 小顶堆. 通过 stack 使其降序输出
    stack = []
    while not q.empty():
        stack.append(q.get())
    print('rank\tcount\turl')
    print('====================')
    for idx, item in enumerate(reversed(stack)):
        print('{}\t{}\t{}'.format(idx, item.cnt, item.url))

