"""A dummy StatsBomb API for testing Sbapi without a StatsBomb subscription.

The server mirrors the real API: the same url paths, the same HTTP basic
authentication, and the same JSON payloads. It serves test data files laid out as
``{kind}/v{version}/{match_id}.json``, plus ``matches/v{version}/matches.json`` and
``competitions/v{version}/competitions.json``, below a data directory, so the
tests exercise the whole path from url building through authentication to the SQL.

Examples
--------
>>> with DummyStatsBombAPI('tests/data') as api:
...     parser = Sbapi(sb_username=api.username, sb_password=api.password,
...                    url=api.url, cache_enabled=False)
...     events = parser.match_data(1001, kind='events')
"""

import json
import re
import threading
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

__all__ = ['DummyStatsBombAPI']

# the match level endpoints: an api path fragment and the data directory
MATCH_ENDPOINTS = {
    'events': 'events',
    'lineups': 'lineups',
    '360-frames': 'threesixty',
}


class DummyStatsBombAPI:
    """A StatsBomb API test double, serving test data files over HTTP basic auth.

    Parameters
    ----------
    data_dir : str or pathlib.Path
        A directory of StatsBomb JSON files laid out as
        ``{kind}/v{version}/{match_id}.json``, e.g. ``events/v11/1001.json``, plus
        ``matches/v{version}/matches.json`` and
        ``competitions/v{version}/competitions.json``.
    username, password : str, defaults 'user', 'passwd'
        The credentials the server requires.
    alias_match_ids : bool, default False
        If True, a match level request for a match without a file is served the
        file that is available. The test data usually covers a single match, so this
        allows testing reads that span several matches. The parser takes the match_id
        from the url rather than the payload, so the rows are still labelled with the
        match that was asked for.
    max_matches : int, default None
        Truncate the matches endpoint to this many matches, to keep the tests that
        read a whole competition small.

    Attributes
    ----------
    url : str
        The base url to pass to Sbapi, e.g. 'http://127.0.0.1:54321/api'.
    requests : list of tuple of str
        Every (path, authorization header) the server received, for asserting on
        the urls and credentials the parser sent.
    """

    def __init__(
        self,
        data_dir,
        username='user',
        password='passwd',
        alias_match_ids=False,
        max_matches=None,
    ):
        self.data_dir = Path(data_dir)
        self.username = username
        self.password = password
        self.alias_match_ids = alias_match_ids
        self.max_matches = max_matches
        self.requests = []
        self._raw_cache = {}
        self._server = None
        self._thread = None

    @property
    def url(self):
        """The base url of the running server."""
        if self._server is None:
            raise RuntimeError(
                'the server is not running: use DummyStatsBombAPI as a context manager'
            )
        host, port = self._server.server_address[:2]
        return f'http://{host}:{port}/api'

    def _expected_authorization(self):
        """The Authorization header value a correctly authenticated request sends."""
        token = b64encode(f'{self.username}:{self.password}'.encode()).decode()
        return f'Basic {token}'

    def _read_file(self, name):
        """Return a parsed test data file, or None if it does not exist.

        Parameters
        ----------
        name : str
            A file path within the data directory, e.g. 'matches/v6/matches.json'.
        """
        path = self.data_dir / name
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def _payload(self, path):
        """Return the JSON payload for an api path, or None for an unknown path.

        Parameters
        ----------
        path : str
            The requested url path, e.g. '/api/v11/events/1001'.
        """
        match = re.fullmatch(r'/api/v(\d+)/competitions', path)
        if match:
            return self._read_file(f'competitions/v{match.group(1)}/competitions.json')

        # a season that is not in the test data is served an empty list
        match = re.fullmatch(r'/api/v(\d+)/competitions/(\d+)/seasons/(\d+)/matches', path)
        if match:
            version, competition_id, season_id = match.groups()
            matches = self._read_file(f'matches/v{version}/matches.json')
            if matches is None:
                return None
            matches = [
                m
                for m in matches
                if m['competition']['competition_id'] == int(competition_id)
                and m['season']['season_id'] == int(season_id)
            ]
            return matches[: self.max_matches]

        match = re.fullmatch(r'/api/v(\d+)/([\w-]+)/(\d+)', path)
        if match:
            version, endpoint, match_id = match.groups()
            if endpoint not in MATCH_ENDPOINTS:
                return None
            kind = MATCH_ENDPOINTS[endpoint]
            file = self.data_dir / kind / f'v{version}' / f'{match_id}.json'
            if not file.exists() and self.alias_match_ids:
                file = self._any_file(kind, version)
            if file is None or not file.exists():
                return None
            return self._raw(file)

        return None

    def _any_file(self, kind, version):
        """Return the path of whichever file exists for a data type, whatever its match.

        Parameters
        ----------
        kind : str
            A data directory, e.g. 'events'.
        version : str
            The requested data version.
        """
        for path in sorted(self.data_dir.glob(f'{kind}/v{version}/*.json')):
            return path
        return None

    def _raw(self, path):
        """Return a file's bytes as they are on disk, cached until the file changes.

        The match level files are several megabytes and duckdb requests them in
        parallel, so parsing and re-serialising them per request is too slow.

        Parameters
        ----------
        path : pathlib.Path
        """
        key = (path, path.stat().st_mtime_ns)
        if key not in self._raw_cache:
            self._raw_cache[key] = path.read_bytes()
        return self._raw_cache[key]

    def __enter__(self):
        api = self

        class Handler(BaseHTTPRequestHandler):
            """Serves the test data, rejecting unauthenticated requests like the API."""

            protocol_version = 'HTTP/1.1'

            def log_message(self, format, *args):
                """Silence the default logging to stderr."""

            def _send(self, status, body=b'', body_only_headers=False):
                """Send a response, omitting the body itself for a HEAD request."""
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Accept-Ranges', 'bytes')
                self.end_headers()
                if body and not body_only_headers:
                    self.wfile.write(body)

            def _resolve(self):
                """Return the (status, body) for the request, recording it first."""
                authorization = self.headers.get('Authorization')
                api.requests.append((self.path, authorization))
                if authorization != api._expected_authorization():
                    return 401, b'{"error": "unauthorized"}'
                payload = api._payload(self.path)
                if payload is None:
                    return 404, b'{"error": "not found"}'
                if isinstance(payload, bytes):
                    return 200, payload
                return 200, json.dumps(payload).encode()

            def do_GET(self):
                status, body = self._resolve()
                start, end = self._range(len(body))
                # only a successful response is ranged: an error keeps its own status
                if status != 200 or start is None:
                    self._send(status, body)
                else:
                    self._send_range(body, start, end)

            def do_HEAD(self):
                """DuckDB sends a HEAD request before reading a file."""
                status, body = self._resolve()
                self._send(status, body, body_only_headers=True)

            def _range(self, length):
                """Parse a Range header into (start, end), or (None, None) if absent."""
                header = self.headers.get('Range')
                if not header or not header.startswith('bytes='):
                    return None, None
                start, _, end = header[len('bytes=') :].partition('-')
                start = int(start) if start else 0
                end = int(end) if end else length - 1
                return start, min(end, length - 1)

            def _send_range(self, body, start, end):
                """Send a 206 partial response, as DuckDB reads files in ranges."""
                chunk = body[start : end + 1]
                self.send_response(206)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(chunk)))
                self.send_header('Content-Range', f'bytes {start}-{end}/{len(body)}')
                self.end_headers()
                self.wfile.write(chunk)

        self._server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
        self._server = None
        self._thread = None
        return False
