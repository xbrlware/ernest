#!/usr/bin/env python

from bs4 import BeautifulSoup

import pdfquery
import re
import urllib

btypes = re.compile('(.*)(Inc|Llc|Comp|Ltd|Corp|Co)(\.)')
aka = re.compile('(./k/a) ([A-Za-z\.\, ]+)')


def regex_filter(words):
    if len(words) > 2:
        m = re.search(btypes, words)
        if m:
            return m.group(0)
        else:
            m = re.search(aka, words)
            if m:
                return m.group(2)
            else:
                return None
    else:
        return None

response = urllib.request.urlopen('http://www.sec.gov/litigation/suspensions/2008/34-58589-o.pdf')

with open('/tmp/todd.pdf', 'wb') as outf:
    outf.write(response.read())

pdf = pdfquery.PDFQuery('/tmp/todd.pdf',
                        merge_tags=('LTChar'),
                        round_floats=True,
                        round_digits=3,
                        input_text_formatter=None,
                        normalize_spaces=False,
                        resort=True,
                        parse_tree_cacher=None,
                        laparams={'all_texts': True, 'detect_vertical': False})

pdf.load()

pdf.tree.write('/tmp/todd.xml')

with open('/tmp/todd.xml', 'r') as inf:
    x = inf.read()

soup = BeautifulSoup(x, 'xml')

i = 0
cache = {}

for ele in soup.findAll('LTTextLineHorizontal'):
    if i > 0:
        if ele['x0'] == cache['x0'] and ele['height'] == cache['height']:
            if re.search(btypes, ele.text):
                m = re.search(btypes, ele.text)
                print(m.group(1) + m.group(2))
                i += 1
            elif re.search(aka, ele.text):
                m = re.search(aka, ele.text)
                print(m.group(2))

        cache = ele
    else:
        cache = ele
        i += 1
"""
x = pdf.pq('LTTextLineHorizontal')

for ele in x:
    m = regex_filter(ele.text)
    if m:
        print(m)
    if len(ele.text) > 2:
        m = re.search(btypes, ele.text)
        if m:
            print(m.group(1) + m.group(2))
        else:
            m = re.search(aka, ele.text)
            if m:
                print(m.group(2))
"""
