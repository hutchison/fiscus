#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import subprocess


def check_latin1_file_encoding(filename):
    ftyp = str(subprocess.check_output(['file', '-b', filename]))
    return 'ISO-8859' in ftyp


def convert_latin1file_to_utf8(filename):
    name, ext = os.path.splitext(filename)
    filename = name + '-utf8.' + ext
    with open(filename, 'w') as f:
        ret = subprocess.call(['iconv', '-f LATIN1 -t UTF-8', filename],
                stdout=f)
        assert ret == 0


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()
    pass

if __name__ == '__main__':
    main()
