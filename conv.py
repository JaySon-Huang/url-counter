#!/bin/env python
#encoding=utf-8

from __future__ import print_function

import os
import sys

def conv(filename, outfile):
    with open(filename, 'r') as infile, open(outfile, 'wb') as outfile:
        for lineno, line in enumerate(infile):
            _, url = line.strip().split(',')  # 提取出 url
            if lineno < 10:
                # 生成 skew keys
                num_repeat = (1 << (20 - lineno))
                for _ in range(num_repeat):
                    print(url, file=outfile)
            else:
                print(url, file=outfile)


if __name__ == '__main__':
    # parse args
    import argparse
    parser = argparse.ArgumentParser(description='input files convert')
    parser.add_argument('--filename', required=True,
                        help='url file to process')
    parser.add_argument('--out', required=True, help='url file')
    args = parser.parse_args()

    conv(args.filename, args.out)
