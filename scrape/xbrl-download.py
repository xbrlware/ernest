#!/usr/bin/env python

'''
    Download new xbrl filings from the edgar RSS feed

    ** Note **
    This runs prospectively for the current year and
    month as new documents are added to the feed each day
'''

import argparse
import datetime
import feedparser
import json
import logging
import os
import os.path
import xml.etree.ElementTree as ET
import zipfile

from elasticsearch import Elasticsearch
from modules.http_handler import HTTP_HANDLER
from generic.logger import LOGGER
from generic.zip_directory_handler import ZIP_DIRECTORY_HANDLER


class XBRL_DOWNLOAD:
    def __init__(self, args, parent_logger):
        self.year = args.year
        self.month = args.month
        self.args = args
        self.hh = HTTP_HANDLER('xbrl_download')
        self.session = self.hh.create_session()
        self.logger = logging.getLogger(parent_logger + '.xbrl_download')
        self.zdh = ZIP_DIRECTORY_HANDLER()

        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            "host": config['es']['host'],
            "port": config['es']['port']}])

    def downloadfile(self, sourceurl, targetfname):
        write_directory = '/'.join(targetfname.split('/')[:-1]).replace(
            'filings', 'unzipped')
        if os.path.isfile(targetfname):
            self.logger.warning('[EXISTS]|{}'.format(targetfname))
            self.logger.info("[UNZIP]|{}".format('unzipping file'))
            self.zdh.unzip_file(targetfname, write_directory)
            rv = True
        else:
            self.logger.info("[DOWNLOADING]|{}".format(sourceurl))
            mem_file = self.hh.get_page(self.session, sourceurl, "binary")
            output = open(targetfname, 'wb')
            output.write(mem_file)
            output.close()
            self.logger.info("[UNZIP]|{}".format('unzipping file'))
            self.zdh.unzip_file(targetfname, write_directory)
            rv = False
        return rv

    def get_url(self, edgarFilingsFeed):
        feedData = self.hh.get_page(self.session, edgarFilingsFeed, "text")
        if feedData is not None:
            root = ET.fromstring(feedData)
        else:
            root = None

        feed = feedparser.parse(feedData)

        return root, feed

    def zip_file_name(self, filings_dir, item):
        linkname = item["link"].split('/')[-1]
        linkbase = os.path.splitext(linkname)[0]
        cik = item["edgar_ciknumber"]
        return filings_dir + cik + '-' + linkbase + "-xbrl.zip"

    def download_enclosures(self, item, url, filings_dir):
        cik = item["edgar_ciknumber"]
        targetfname = filings_dir + cik + '-' + url.split('/')[-1]
        self.downloadfile(url, targetfname)

    def manual_xbrl(self, item, itemIndex, root, f_dir):
        zipfname = self.zip_file_name(f_dir, item)
        if os.path.isfile(zipfname):
            self.logger.warning("[EXISTS]|{}".format("Local copy exists"))
        else:
            edgarNamespace = {'edgar': 'http://www.sec.gov/Archives/edgar'}
            currentItem = list(root.iter("item"))[itemIndex]
            xbrlFiling = currentItem.find("edgar:xbrlFiling", edgarNamespace)
            xbrlFilesItem = xbrlFiling.find("edgar:xbrlFiles", edgarNamespace)
            xbrlFiles = xbrlFilesItem.findall("edgar:xbrlFile", edgarNamespace)

            if not os.path.exists(f_dir + "temp"):
                os.makedirs(f_dir + "temp")
                zf = zipfile.ZipFile(zipfname, "w")
                try:
                    for x in xbrlFiles:
                        xurl = x.get("{http://www.sec.gov/Archives/edgar}url")
                        if xurl.endswith((".xml", ".xsd")):
                            targetfname = f_dir + "temp/" + xurl.split('/')[-1]
                            good_read = self.downloadfile(xurl, targetfname)
                            if not good_read:
                                zf.write(targetfname,
                                         xurl.split('/')[-1],
                                         zipfile.ZIP_DEFLATED)
                                os.remove(targetfname)
                finally:
                    zf.close()
                    os.rmdir(f_dir+"temp")

    def SECdownload(self, year, month, day):
        url_base = \
            'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-{0}-{1}.xml'
        filings_dir = "/home/ubuntu/sec/filings__{0}__{1}/".format(year, month)
        edgarFilingsFeed = url_base.format(year, str(month).zfill(2))
        if not os.path.exists(filings_dir):
            os.makedirs(filings_dir)
        # we have to unfortunately use both feedparser
        # (for normal cases) and ET for old-style RSS feeds,
        # because feedparser cannot handle the case where
        # multiple xbrlFiles are referenced without enclosure

        root, feed = self.get_url(edgarFilingsFeed)
        itemIndex = 0
        # Process RSS feed and walk through all items contained
        for item in feed.entries:
            if day and str(item['published_parsed'].tm_mday) == day:
                try:
                    # Identify ZIP file enclosure, if available
                    enclosures = [l for l in item["links"]
                                  if l["rel"] == "enclosure"]

                    if (len(enclosures) > 0):
                        # ZIP file enclosure exists, just download the ZIP file
                        self.download_enclosures(item,
                                                 enclosures[0]['href'],
                                                 filings_dir)
                    else:
                        # We need to manually download all XBRL
                        # files and ZIP them ourselves...
                        self.manual_xbrl(item, itemIndex, root, filings_dir)
                except:
                    self.logger.error('[DOWNLOAD]|{}'.format("main fail"))
                itemIndex += 1

    def main(self):
        if self.args.year:
            year = str(args.year)
        else:
            year = str(datetime.datetime.now().year)

        if self.args.month:
            month = str(args.month)
        else:
            month = str(datetime.datetime.now().month)

        if self.args.day:
            day = str(args.day)
        else:
            day = str(datetime.datetime.now().day - 1)

        self.SECdownload(year, month, day)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='download-xbrl-rss-docs')
    parser.add_argument("--year",  type=str, action='store')
    parser.add_argument("--month",  type=str, action='store')
    parser.add_argument("--day", type=str, action='store')
    parser.add_argument("--config-path",
                        type=str, action='store', default='../config.json')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    args = parser.parse_args()
    logger = LOGGER('xbrl', args.log_file).create_parent()
    xd = XBRL_DOWNLOAD(args, 'xbrl')
    xd.main()
