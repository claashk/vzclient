import asyncio
import csv

class FileDriver:
    def __init__(self, file, format="csv", **kwargs):
        self._config = dict(file=file)
        self._format = format
        self._parser_config = dict(**kwargs)
        self._parser = None
        self._file = None

        if self._format == "csv":
            self._config['newline'] = ""
        else:
            raise RuntimeError(f"Unsupported file format: '{format}'")

    async def __aenter__(self):
        if self.is_connected:
            raise RuntimeError("Client already in use")
        self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def is_connected(self):
        """Check if file driver is connected to a file

        Return:
            bool: True if and only if client is connected
        """
        return self._file is not None

    def connect(self, **kwargs):
        """Connect to a database

        Arguments:
            **kwargs: Keyword arguments used for the connection. These
                arguments will override any arguments passed to init.
        """
        self._config.update(kwargs)
        self.disconnect()
        self._file = open(**self._config)
        if "w" in self._file.mode:
            self._parser = csv.writer(self._file, **self._parser_config)
        else:
            self._parser = csv.reader(self._file, **self._parser_config)

    def disconnect(self):
        """Disconnect client"""
        if self.is_connected:
            self._file.close()
            self._file = None

    def get_reader(self):
        client = self.get_client()
        client.init_reader()
        return client

    def get_writer(self):
        client = self.get_client()
        client.init_writer()
        return client

    def get_client(self):
        if self.is_connected:
            return FileDriver(format=self._format, **self._config)
        return self

    def init_writer(self):
        # self._config[mode] = ...
        raise NotImplementedError("Reader")

    def init_reader(self):
        raise NotImplementedError("Reader")

    async def iter_chunks(self,
                          channel,
                          begin=None,
                          end=None,
                          chunk_size=8192):
        # TODO begin, end have to be implemented
        while True:
            chunk = [(row[0], row[1]) for row in self.read_chunk(chunk_size)]
            if len(chunk) == 0:
                return
            yield chunk
            await asyncio.sleep(0)

    def read_chunk(self, size):
        for i, line in enumerate(self._parser):
            if i >= size:
                return
            yield line

    async def write_chunk(self, chunk):
        for t, x in chunk:
            self._parser.writerow((t, x))
            await asyncio.sleep(0)

