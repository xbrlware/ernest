#!/usr/bin/env python2.7

import requests
import logging


class HTTP_HANDLER:
    def __init__(self, parent_logger):
        self.logger = logging.getLogger(parent_logger + '.http_handler')
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Ubuntu Chromium/56.0.2924.76 \
            Chrome/56.0.2924.76 Safari/537.36"

    def create_session(self):
        s = requests.Session()
        s.headers.update({"User-Agent": self.user_agent})
        return s

    def handle_request(self, session, url, tries):
        try:
            req = session.get(url)
            return req
        except requests.exceptions.ConnectionError as e:
            if tries < 10:
                self.logger.info("[CONNECTIONERROR]|retrying: {}".format(url))
                tries += 1
                self.handle_request(session, url, tries)
            else:
                self.logger.debug(
                    "[CONNECTIONERROR]|[{0}]|{1}".format(e, url))
                return None
        except requests.exceptions.HTTPError as e:
            self.logger.debug(
                "[{0}]|[{1}]|{2}".format(e.status_code, e, url))
            return None
        except requests.exceptions.InvalidURL as e:
            self.logger.debug("[INVALIDURL|{0}|{1}".format(e, url))
            return None
        except requests.exceptions.Timeout as e:
            self.logger.debug("[TIMEOUT]|{0}|{1}".format(e, url))
            return None
        except:
            self.logger.debug("[UNSPECIFIEDERROR]|{0}|{1}".format(
                'error', url))
            return None

    def get_page(self, session, url, fmt_type="text"):
        r = self.handle_request(session, url, 0)
        if r is not None:
            if fmt_type == "json":
                rv = r.json()
            elif fmt_type == "text":
                rv = r.text
            elif fmt_type == "binary":
                rv = r.content
        else:
            rv = r
        return rv
