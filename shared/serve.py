"""Start a FastAPI service with uvicorn, and tell the app WHO is calling.

  python -m shared.serve gateway.main:app

With mutual TLS on, every caller shows a certificate. Uvicorn checks it but does not pass
its name to the application, so this launcher does: for each connection it reads the
certificate's common name (e.g. "farm-a", "operator") and puts it in the request scope
as "client_cn". The access rules in shared/access.py use it.
With TLS off it simply runs uvicorn as before.
"""
import os
import sys

import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol


def common_name(ssl_object) -> str | None:
    cert = ssl_object.getpeercert() if ssl_object else None
    for rdn in (cert or {}).get("subject", ()):
        for key, value in rdn:
            if key == "commonName":
                return value
    return None


class PeerAwareProtocol(H11Protocol):
    """Uvicorn's HTTP/1.1 protocol, plus the caller's certificate name in every request."""

    def connection_made(self, transport):
        super().connection_made(transport)
        cn = common_name(transport.get_extra_info("ssl_object"))
        app = self.app

        async def with_caller(scope, receive, send):
            scope["client_cn"] = cn
            await app(scope, receive, send)

        self.app = with_caller


def main():
    target = sys.argv[1]
    tls_args = {}
    if os.getenv("UVICORN_SSL_CERTFILE"):
        tls_args = dict(ssl_certfile=os.environ["UVICORN_SSL_CERTFILE"],
                        ssl_keyfile=os.environ["UVICORN_SSL_KEYFILE"],
                        ssl_ca_certs=os.getenv("UVICORN_SSL_CA_CERTS"),
                        ssl_cert_reqs=int(os.getenv("UVICORN_SSL_CERT_REQS", "0")),
                        http=PeerAwareProtocol)
    uvicorn.run(target, host="0.0.0.0", port=int(os.getenv("PORT", "8000")), **tls_args)


if __name__ == "__main__":
    main()
