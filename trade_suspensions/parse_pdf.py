#!/usr/bin/env python

from bs4 import BeautifulSoup
import re

starter = 'ORDER OF SUSPENSION OF TRADING'
ender = 'the Matter of'
found = False
c = []

btypes = re.compile('^ *Inc.|Llc.|Comp.|Ltd. *$')
business_types = ['Llc.', 'Ltd.', 'Corp.', 'Inc.']

with open('2014.xml', 'r') as inf:
    x = inf.read()

soup = BeautifulSoup(x, 'xml')

"""
for ele in soup.findAll(re.compile(r'LTTextBox*')):
    if starter in ele.text:
        found = True
    elif ender in ele.text:
        break
    elif found and len(ele.text) > 1:
        c.append(ele.text)

if len(c) == 0:
    for ele in soup.findAll(re.compile(r'LTTextBox*')):
        if ender in ele.text:
            found = True
        elif '500-1' in ele.text:
            break
        elif found and len(ele.text) > 1:
            c.append(ele.text)
"""

c = soup.findAll()[0]['Title']
d = c.split(',')

e = [x.strip() for x in d]
f = []
i = 0

for ele in e:
    if len(ele) > 0 and not re.match(btypes, ele):
        if ele[:4] == 'and ':
            ele = ele[4:]

        retest = re.search(r'\(.+/k/a', ele)
        if retest:
            if retest.span(0)[0] == 0:
                ele = ele[retest.span(0)[1]:-1].strip()
            else:
                f.append(ele[:retest.span(0)[0]].strip())
                f.append(ele[retest.span(0)[1]:-1].strip())
                continue

        if len(ele) == 1:
            f[i - 1] = ele + f[i - 1]
        else:
            f.append(ele)

print('this is f --> ', f)
