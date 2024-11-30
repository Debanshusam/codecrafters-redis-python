import logging as logger
from typing import Callable

from app.packages.cmd_router import ping_cmd, echo_cmd, set_get_cmd

IMPL_NO_ARG_TYP_CMD_MAP: dict[str, Callable[[], bytes]] = {
    "ping": ping_cmd.ping_impl,
}

IMPL_ARG_TYP_CMD_MAP: dict[str, Callable[[bytes], bytes]] = {
    "echo": echo_cmd.echo_impl,
    "set": set_get_cmd.set_impl,
    "get": set_get_cmd.get_impl,
}


def redis_no_arg_typ_cmd_router(redis_cmd: bytes) -> bytes:
    _redis_cmd: str = redis_cmd.decode(encoding="utf-8")

    if "ping" == _redis_cmd.lower():
        logger.info(f"PING Command detected: {_redis_cmd}")
        return ping_cmd.ping_impl()
    else:
        return b''


def redis_arg_typ_cmd_router(redis_cmd: bytes, cmd_args: bytes) -> bytes:
    _redis_cmd: str = redis_cmd.decode(encoding="utf-8")

    if "echo" == _redis_cmd.lower():
        logger.info(f"Command detected: {_redis_cmd}")
        return echo_cmd.echo_impl(cmd_args)

    elif "set" == _redis_cmd.lower():
        logger.info(f"Command detected: {_redis_cmd}")
        return set_get_cmd.set_impl(cmd_args)

    elif "get" == _redis_cmd.lower():
        logger.info(f"Command detected: {_redis_cmd}")
        return set_get_cmd.get_impl(cmd_args)

    else:
        return b''
