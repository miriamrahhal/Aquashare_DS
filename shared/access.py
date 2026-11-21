"""Authorization: what each certificate is allowed to do.

Mutual TLS tells us WHO is calling (the name on its certificate). These rules decide
WHAT it may do. They only apply when TLS is on; with TLS off everything works as before.

  gateway   open a day, register a farm ........ operator, dashboard only
            register a key, report consumption . the farm itself
            send a transfer .................... the sending farm
            read anything ...................... every certificate
            (operator and dashboard are the administrators: they may do everything)
  ledger    submit a command ................... gateways, other ledger nodes, operator

Requests that pass through the load balancer arrive with the load balancer's certificate,
so Nginx adds the real caller's name in the X-Client-CN header. That header is trusted
only when the connection really comes from the load balancer.
"""
from fastapi import HTTPException, Request

from shared import tls

LOAD_BALANCER = "gateway-lb"
ADMINS = {"operator", "dashboard"}


def caller(request: Request) -> str | None:
    cn = request.scope.get("client_cn")
    if cn == LOAD_BALANCER:
        cn = request.headers.get("x-client-cn") or None
    return cn


def require(request: Request, allowed: set[str], action: str):
    if not tls.ENABLED:
        return
    cn = caller(request)
    if cn in ADMINS or cn in allowed:
        return
    raise HTTPException(403, f"certificate '{cn}' is not allowed to {action}")


def require_prefix(request: Request, prefixes: tuple[str, ...], action: str):
    if not tls.ENABLED:
        return
    cn = caller(request) or ""
    if cn == "operator" or cn.startswith(prefixes):
        return
    raise HTTPException(403, f"certificate '{cn}' is not allowed to {action}")
