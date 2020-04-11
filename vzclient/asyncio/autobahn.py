import asyncio

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.types import EncodedPayload
from autobahn.wamp.interfaces import IPayloadCodec


class VzLoggerCodec(object):
    """Raw values used by VZlogger
    """
    def __init__(self, with_timestamp=False):
        self.with_timestamp = with_timestamp

    def encode(self, is_originating, uri, args=None, kwargs=None):
        # Autobahn wants to send custom payload: convert to an instance
        # of EncodedPayload
        if self.with_timestamp:
            kwargs['value'] = kwargs.pop('reading', -1.)
            payload = json.dumps(kwargs).encode()
        else:
            payload = str(kwargs.get('reading', -1.)).encode()
        return EncodedPayload(payload, u'mqtt')

    def decode(self, is_originating, uri, encoded_payload):
        # Autobahn has received a custom payload.
        # convert it into a tuple: (uri, args, kwargs)
        if self.with_timestamp:
            kwargs = json.loads(encoded_payload.payload.decode())
            kwargs['reading'] = kwargs.pop('value', -1.)
            kwargs['uri'] = uri
            kwargs.setdefault('timestamp')
        else:
            kwargs = dict(reading=float(encoded_payload.payload),
                          uri=uri,
                          timestamp=None)
        return uri, [], kwargs


# we need to register our codec!
IPayloadCodec.register(VzLoggerCodec)

