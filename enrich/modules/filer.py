from ftp_parser import Parser


class Filer:
    suffix_of_interest = '.hdr.sgml'
    content = []
    content_parsed = []
    parser = Parser()

    def __init__(self, cik, connection):
        self.cik = str(int(cik))
        self.connection = connection
        self._go_to_directory()

    def _go_to_directory(self):
        self.connection.cwd('/edgar/data/' + self.cik)

    def ls(self):
        return self.connection.dir()

    def ls_d(self):
        return self.connection.retrlines('LIST */')

    def files_of_interest(self):
        cmd = 'LIST */*/*' + self.suffix_of_interest
        lines = []
        self.connection.retrlines(cmd, lambda x: self._process_foi(x, lines))
        return lines

    def _process_foi(self, line, lines):
        lines.append(line.split(' ')[-1])

    def download_file(self, path):
        x = []
        self.connection.retrbinary('RETR ' + path, x.append)
        return ''.join(x)

    def download_foi(self):
        foi = self.files_of_interest()
        for f in foi:
            print('downloading ' + f)
            self.content.append(self.download_file(f))
        return self.content

    def parse_foi(self):
        self.content_parsed = [self.parser.run_header(c) for c in self.content]
        return self.content_parsed
