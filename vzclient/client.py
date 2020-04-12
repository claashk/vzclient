import requests


class Client(object):
    """Client for the Volkszaehler API

    A simple client for the `Volkszaehler`_ middleware, which allows to access
    and manipulate data via the REST API.

    `Volkszaehler API reference`_

    Arguments:
        url (str): URL of the volkszähler middleware API

    .. _Volkszaehler:
        https://volkszaehler.org

    .. _Volkszaehler API reference:
        https://wiki.volkszaehler.org/development/api/reference
    """
    def __init__(self, url):
        self.api = url

    @property
    def api(self):
        """Read access to the middleware API url

        Return:
            str: url of middleware API
        """
        return self._api

    @api.setter
    def api(self, url):
        """Set middleware API url

        Arguments:
            url (str): Url of the middleware
        """
        self._api = str(url)
        if self._api.endswith("/"):
            self._api = self.api[:-1]

    def get(self, *args, **kwargs):
        """Get information from volkszaehler middleware

        Shortcut for a GET request.

        Arguments:
            *args (list): Positional arguments passed verbatim to
                 :meth:`~vokszaehler.Client.req`
            **kwargs (dict): Keyword arguments passed verbatim to
                 :meth:`~vokszaehler.Client.req`

        Return:
            requests.Response: `requests.Response`_ object containing
        """
        return self.req("GET", *args, **kwargs)

    def add(self, *args, **kwargs):
        return self.req("POST", *args, **kwargs)

    def edit(self, *args, **kwargs):
        return self.req("PUT", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.req("DELETE", *args, **kwargs)

    def list_channels(self):
        """Get list of available (public) channels

        Return:
            dict: Dictionary containing channel information
        """
        reply = self.get("channel", format="json")
        if not reply.ok:
            raise ConnectionError("html error", reply.status_code)
        return reply.json()["channels"]

    def url(self, context, id, format):
        """Create volkszaehler url

        Assembles the `volkszaehler url`_ from its components.

        Arguments:
            context (str): `volkszaehler context`_.
            id (str): UUID or name of entity or group
            format (str): Output format of the `volkszaehler reply`

        Return:
            str: url
        """
        if not context:
            raise ValueError("Context must not be empty")

        if not format:
            raise ValueError("format string may not be empty")

        for arg in [context, id, format]:
            if arg and ("." in arg or "/" in arg):
                raise ValueError("Invalid characters in context string")

        return "{}/{}{}.{}".format(self.api,
                                   context,
                                   "/{}".format(id) if id else "",
                                   format)

    def req(self, method, context="capabilities", id=None, format="json", **kwargs):
        """Generic request creation routine

        Arguments:
            method (str): Name of the `volkzaehler method`_ to use. The name is
                passed verbatim to `requests.request`_
            context (str): Name of the `volkszaehler context`_. Defaults to
                ``"capabilities"``.
            id (str): Identifier. Either UUID or a name as described in the
                `volkszaehler url`_ description.
            format (str): Output format. See `volkszaehler reply`_ description
                for details. Defaults to ``"json"``
            **kwargs (dict): Additional keyword arguments forwarded directly to
                the `requests` method

        Return:
            requests.Response: A `requests.Response`_ object containing the
                result of the query.

        .. _requests.request:
            https://2.python-requests.org/en/master/api/#requests.request

        .. _volkszaehler method:
            https://wiki.volkszaehler.org/development/api/reference#http-methode

        .. _volkszaehler context:
            https://wiki.volkszaehler.org/development/api/reference#kontexte

        .. _volkszaehler url:
            https://wiki.volkszaehler.org/development/api/reference#url

        .. _volkszaehler reply:
            https://wiki.volkszaehler.org/development/api/reference#antwort

        .. _requests Response:
            https://2.python-requests.org/en/master/api/#requests.Response
        """
        return requests.request(method=method,
                                url=self.url(context, id, format),
                                **kwargs)
