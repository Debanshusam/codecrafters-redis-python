import logging as logger
import asyncio


def _encode_string_as_resp2(string: str) -> bytes:
    """Encodes a string into a RESP2 string.

    Args:
        string: The string to encode.

    Returns:
        The encoded RESP2 string.
    """
    resp_string: str = f"+{string}\r\n"
    return resp_string.encode(encoding='utf-8')


def _extract_cmd_resp_bulk_array(resp_bulk_array: bytes) -> list[str]:

    _resp_to_str: str = resp_bulk_array.decode("utf-8")
    _commands: list[str] = _resp_to_str.splitlines()
    _commands_copy: list[str] = _commands[::]

    logger.debug(_commands_copy)

    for ele in _commands:
        if ele.startswith(("$", "*")):
            _commands_copy.remove(ele)

    return _commands_copy


def _decode_resp_to_string(resp_string: bytes) -> str | list[str]:
    """
    Decodes a simple RESP string into a normal string.

    Args:
        resp_string: The RESP string to decode.

    Returns:
        The decoded string.
    """

    RESP_SEP: bytes = b'\r\n'

    if resp_string.startswith(b'$'):

        # Bulk string
        logger.debug(resp_string[1:].split(RESP_SEP)[0])
        length = int(resp_string[1:].split(RESP_SEP)[0])


        return resp_string[1:].split(RESP_SEP)[1][:length].decode('utf-8')

    elif resp_string.startswith(b'+'):
        # Simple string
        return resp_string[1:].strip().decode('utf-8')

    elif resp_string.startswith(b'*'):
        """RESP Bulk Array
        *<number-of-elements>\r\n<element-1>...<element-n>
        ref: https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
        """

        return _extract_cmd_resp_bulk_array(resp_bulk_array=resp_string)

    else:
        raise ValueError("Unsupported RESP type")


def _ping_implementation() -> str:
    _repsonse: str = "PONG"
    return _repsonse


def _redis_cmd_router(data: str) -> str:
    text_data: str = data

    if "ping" in text_data.lower():
        logger.info(f"PING Command detected: {text_data}")
        return _ping_implementation()
    else:
        return data


async def _ready_client_response(writer: asyncio.StreamWriter, client_input_cmd: str | list[str]) -> None:
    response_data: str = ""
    if isinstance(client_input_cmd, str):
        response_data = _redis_cmd_router(client_input_cmd)
        writer.write(_encode_string_as_resp2(response_data))
        await writer.drain()
    else:
        for cmd in client_input_cmd:
            response_data = _redis_cmd_router(cmd)
            logger.debug(f"response_data = {response_data}")
            writer.write(_encode_string_as_resp2(response_data))
            await writer.drain()


async def _receive_client_input(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        buffered_data: bytes = await reader.read(1024)
        logger.debug(f"buffered_data = {buffered_data}")

        buffered_data_str: str = buffered_data.decode()
        logger.debug(f"buffered_data_str = {buffered_data_str}")

        if not buffered_data_str:
            logger.warning("Empty data received from client")
            break

        client_input_cmd: str | list[str] = _decode_resp_to_string(
            resp_string=buffered_data
            )

        await _ready_client_response(writer, client_input_cmd)

    writer.close()
    await writer.wait_closed()


async def _init_socket_server(sock_add: tuple[str, int]) -> None:
    server: asyncio.Server = await asyncio.start_server(
        client_connected_cb=_receive_client_input,
        host=sock_add[0],
        port=sock_add[1]
        )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logger.basicConfig(level=logger.DEBUG)
    sock_add = ("127.0.0.1", 6379)
    asyncio.run(_init_socket_server(sock_add))
