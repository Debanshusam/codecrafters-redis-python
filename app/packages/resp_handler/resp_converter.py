
""" Redis RESP converter module.

Ref: https://redis.io/docs/latest/develop/reference/protocol-spec/
"""
import logging as logger
from app.packages.cmd_router import cmd_router as CMDROUTE

# Define RESP constants
RESP_BASE_SEP = b'\r\n'
SIMPLE_STR_PREFIX = b'+'
BULK_STR_PREFIX = b'$'
BULK_ARRAY_PREFIX = b'*'


# Unimplemented commands error message
def umimplemented_cmd_err_msg(cmd: str) -> bytes:
    return f"+ERR unknown command '{cmd}'".encode(encoding='utf-8')


def encode_to_resp2(inp: bytes | str) -> bytes:
    """Encodes a string into a RESP2 string.

    Args:
        string: The string to encode.

    Returns:
        The encoded RESP2 string.
    """
    if isinstance(inp, str):
        if (
            not inp.startswith(SIMPLE_STR_PREFIX.decode('utf-8'))
            and
            not inp.startswith(BULK_STR_PREFIX.decode('utf-8'))
            and
            not inp.startswith(BULK_ARRAY_PREFIX.decode('utf-8'))
        ):
            end_crlf_str: str = (
                RESP_BASE_SEP.decode('utf-8')
                if not inp.endswith(RESP_BASE_SEP.decode('utf-8'))
                else ""
            )
            resp_inp: str = SIMPLE_STR_PREFIX.decode() \
                + inp \
                + end_crlf_str

            return resp_inp.encode(encoding='utf-8')
        else:
            return inp.encode(encoding='utf-8')

    else:
        if (
            not bytes(inp).startswith(SIMPLE_STR_PREFIX)
            and
            not bytes(inp).startswith(BULK_STR_PREFIX)
            and
            not bytes(inp).startswith(BULK_ARRAY_PREFIX)
        ):
            end_crlf: bytes = (
                RESP_BASE_SEP if not bytes(inp).endswith(RESP_BASE_SEP)
                else b''
            )

            resp_inp_bytes: bytes = SIMPLE_STR_PREFIX \
                + inp \
                + end_crlf

            return resp_inp_bytes
        else:
            return inp


def handle_simple_string_resp(resp_inp: bytes) -> bytes:
    """
    Handles a simple string RESP.

    - Simple strings never contain carriage
    return (\r) or line feed (\n) characters.

    Args:
        resp_inp: The RESP string to handle.

    Returns:
        The decoded output.
    """

    logger.debug(f"resp_inp = {resp_inp}")

    # checking if the cmd is implemented ?

    _simple_resp_str: str = resp_inp[1:].strip().decode('utf-8') \
        if resp_inp else ''

    if _simple_resp_str.lower() in CMDROUTE.IMPL_NO_ARG_TYP_CMD_MAP:
        return CMDROUTE.redis_no_arg_typ_cmd_router(redis_cmd=resp_inp)
    else:
        logger.error(f"Unimplemented command: {_simple_resp_str}")
        return umimplemented_cmd_err_msg(cmd=_simple_resp_str)


def handle_bulk_string_resp(resp_inp: bytes) -> bytes:
    """
    Handles a bulk string RESP.

    format - r'$\\<length\\>\\r\\n\\<payload_data\\>\\r\\n'

    - Bulk strings can contain any binary data and
    may also be referred to as binary or blob.
    Note that bulk strings may be further encoded and decoded,
    e.g. with a wide multi-byte encoding, by the client.

    Args:
        resp_inp: The RESP string to handle.

    Returns:
        The encoded bytes output.
    """
    logger.debug(f"resp_inp = {resp_inp}")

    _bulk_resp_str_payload: bytes = resp_inp.split(RESP_BASE_SEP)[1]

    logger.debug(f"_bulk_resp_str_payload = {_bulk_resp_str_payload}")
    # TODO - We are simply returning the payload as is
    return resp_inp


def handle_bulk_array_resp(resp_inp: bytes) -> list[bytes]:
    """
    Handles a bulk array RESP.

    NOTE: This takes of consolidating the responses for each command

    - RESP Bulk Array
        *<number-of-elements>\r\n<element-1>...<element-n>
        ref:
        https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays

        Aggregates, such as Arrays and Maps,
        can have varying numbers of sub-elements and nesting levels.

    - example *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"]

    Args:
        resp_inp: The RESP string to handle.

    Returns:
        The decoded output.
    """
    logger.debug(f"resp_inp = {resp_inp}")
    _commands: list[bytes] = resp_inp.split(RESP_BASE_SEP) if resp_inp else []
    _commands_copy: list[bytes] = _commands[::]

    logger.debug(f"_commands_copy = {_commands_copy}")

    # Reading the Bulk Array
    logger.debug(f"bulk arrary size: {_commands[0]}")

    _consolidated_responses: list[bytes] = []
    _redis_cmd: bytes

    for idx in range(1, len(_commands)):

        ele: str = _commands[idx].decode('utf-8')

        if ele.lower() in CMDROUTE.IMPL_NO_ARG_TYP_CMD_MAP:
            _redis_cmd = _commands[idx]

            _consolidated_responses.append(
                CMDROUTE.redis_no_arg_typ_cmd_router(redis_cmd=_redis_cmd)
            )

        elif ele.lower() in CMDROUTE.IMPL_ARG_TYP_CMD_MAP:
            _redis_cmd = _commands[idx]
            _redis_cmd_args: bytes = b""

            # Reading next consecutives to treak them as args
            # if they are not implemented as commands
            for arg_idx in range(idx+1, len(_commands)):

                next_ele: str = _commands[arg_idx].decode('utf-8')

                if (
                    next_ele.lower() not in CMDROUTE.IMPL_NO_ARG_TYP_CMD_MAP  # noqa: E501
                    and
                    next_ele.lower() not in CMDROUTE.IMPL_ARG_TYP_CMD_MAP  # noqa: E501
                ):
                    if next_ele.startswith("$"):
                        # to skip the length of each field
                        pass
                    else:
                        _redis_cmd_args = _redis_cmd_args + RESP_BASE_SEP \
                            + _commands[arg_idx]
                else:
                    break

            logger.debug(f"_redis_cmd={_redis_cmd}")
            logger.debug(f"_redis_cmd_args={_redis_cmd_args}")

            _consolidated_responses.append(
                CMDROUTE.redis_arg_typ_cmd_router(
                    redis_cmd=_redis_cmd,
                    cmd_args=_redis_cmd_args
                )
            )

    return _consolidated_responses


def redis_resp_parser(resp_inp: bytes) -> bytes | list[bytes]:
    """
    Decodes a simple RESP string.

    Extracts the redis command from the decoded RESP string.

    Args:
        resp_inp: The RESP string to decode.

    Returns:
        The decoded output.
    """

    if resp_inp.startswith(SIMPLE_STR_PREFIX):
        # Simple string
        return handle_simple_string_resp(resp_inp=resp_inp)

    elif resp_inp.startswith(BULK_STR_PREFIX):
        # Bulk string
        # TODO : Add loop to handle bulk string responses and concatenate and
        # return
        return handle_bulk_string_resp(resp_inp=resp_inp)

    elif resp_inp.startswith(BULK_ARRAY_PREFIX):
        # RESP Bulk Array
        return handle_bulk_array_resp(resp_inp=resp_inp)

    else:
        raise ValueError("Unsupported RESP type")
