"""TLS contexts for TCP, WebSocket, and HTTP endpoints."""

from __future__ import annotations

import ssl

from lupaxa.switchboard.config import TlsSpec
from lupaxa.switchboard.exceptions import SwitchboardError


def server_context(tls: TlsSpec) -> ssl.SSLContext:
    """A server context. Callers must already require certfile and keyfile."""
    if not tls.certfile or not tls.keyfile:
        raise SwitchboardError("TLS listeners require certfile and keyfile.")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.load_cert_chain(tls.certfile, tls.keyfile)
    except OSError as exc:
        raise SwitchboardError("Could not load the TLS certificate.") from exc
    return context


def client_context(tls: TlsSpec) -> ssl.SSLContext:
    """A client context. Verification stays on unless the configuration disables it."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if tls.verify:
        context.load_default_certs()
        if tls.cafile:
            try:
                context.load_verify_locations(tls.cafile)
            except OSError as exc:
                raise SwitchboardError("Could not load the TLS certificate.") from exc
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        return context
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context
