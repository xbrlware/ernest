import re


class Parser:
    def run_header(self, txt):
        txt = re.sub('\r', '', txt)
        hd = txt[txt.find('<ACCESSION-NUMBER>'):txt.find('<DOCUMENT>')]
        hd = filter(None, hd.split('\n'))
        return self.parse_header(hd)

    def parse_header(self, hd):
        curr = {}
        i = 0
        while i < len(hd):
            h = hd[i]
            if re.search('>.+', h) is not None:
                # Don't descend
                key = re.sub('<|(>.*)', '', h)
                val = re.sub('<[^>]*>', '', h)
                curr[key] = [val]
                i = i + 1
            else:
                if re.search('/', h) is None:
                    key = re.sub('<|(>.*)', '', h)
                    end = filter(
                        lambda i: re.search('</' + h[1:], hd[i]),
                        range(i, len(hd)))
                    tmp = curr.get(key, [])
                    if len(end) > 0:
                        curr[key] = \
                            tmp + [self.parse_header(hd[(i + 1):(end[0])])]
                        i = end[0]
                    else:
                        curr[key] = tmp + [None]
                        i = i + 1
                else:
                    i = i + 1
        return curr
