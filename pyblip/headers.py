##
##

import base64


class BasicAuth(object):

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def header(self):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode('ascii')
        auth_encoded = base64.b64encode(auth_bytes)
        request_header = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }
        return request_header


class SessionAuth(object):

    def __init__(self, session_id):
        self.session = session_id

    def header(self):
        request_header = {
            "Cookie": f"SyncGatewaySession={self.session}",
        }
        return request_header
