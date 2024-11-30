import logging as logger
from typing import Callable

from app.packages.cmd_router import ping_cmd, echo_cmd

IMPL_NO_ARG_TYP_CMD_MAP: dict[str, Callable[[], bytes]] = {
    "ping": ping_cmd.ping_implementation,
}

IMPL_ARG_TYP_CMD_MAP: dict[str, Callable[[bytes], bytes]] = {
    "echo": echo_cmd.echo_implementation,
}


def redis_no_arg_typ_cmd_router(redis_cmd: bytes) -> bytes:
    _redis_cmd: str = redis_cmd.decode(encoding="utf-8")

    if "ping" in _redis_cmd.lower():
        logger.info(f"PING Command detected: {_redis_cmd}")
        return ping_cmd.ping_implementation()
    else:
        return b''


def redis_arg_typ_cmd_router(redis_cmd: bytes, cmd_args: bytes) -> bytes:
    _redis_cmd: str = redis_cmd.decode(encoding="utf-8")

    if "echo" in _redis_cmd.lower():
        logger.info(f"Command detected: {_redis_cmd}")
        return echo_cmd.echo_implementation(cmd_args)
    else:
        return b''