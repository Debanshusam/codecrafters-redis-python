import socket  # noqa: F401
# from loguru import logger
import logging as logger


def _encode_string_as_resp2(string: str) -> bytes:
    """Encodes a string into a RESP2 string.

  Args:
    string: The string to encode.

  Returns:
    The encoded RESP2 string.
  """
    # byte_count: int = len(string.encode(encoding='utf-8'))
    # resp_string: str = f"+{byte_count}\r\n{string}\r\n"
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
    # _repsonse: bytes = _encode_string_as_resp2(string="PONG")

    _repsonse: str = "PONG"

    return _repsonse


def _redis_cmd_router(data: str) -> str:

    # PING-PONG cmd
    # text_data: str = _decode_resp_to_string(resp_string=data)
    text_data: str = data

    if "ping" in text_data.lower():
        logger.info(f"PING Command detected: {text_data}")
        return _ping_implementation()
    else:
        # returning back the input data
        # return _encode_string_as_resp2(string=data)
        return data


def _receive_client_input(sock_conn: socket.socket) -> None:
    while True:
        buffered_data: bytes = sock_conn.recv(1024)
        logger.debug(f"buffered_data = {buffered_data}")

        buffered_data_str: str = buffered_data.decode()
        logger.debug(f"buffered_data_str = {buffered_data_str}")

        if not buffered_data_str:
            logger.warning("Empty data received from client")
            break

        client_input_cmd: str | list[str] = _decode_resp_to_string(buffered_data)

        _ready_client_response(sock_conn, client_input_cmd)


def _ready_client_response(sock_conn: socket.socket, client_input_cmd: str | list[str]):

    respsonse_data: str = ""
    if isinstance(client_input_cmd, str):
        respsonse_data = _redis_cmd_router(client_input_cmd)
        # Write the same data back
        sock_conn.sendall(_encode_string_as_resp2(respsonse_data))

    else:
        for cmd in client_input_cmd:
            respsonse_data = _redis_cmd_router(cmd)
            logger.debug(f"respsonse_data = {respsonse_data}")

            # Write the same data back
            sock_conn.sendall(_encode_string_as_resp2(respsonse_data))


def main():
    # You can use print statements as follows for debugging, t
    # hey'll be visible when running tests.
    logger.info("Logs from your program will appear here!")
    # logger.info("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    socket_address: tuple[str, int] = ("localhost", 6379)
    # server_socket: socket.socket = socket.create_server(
    #     address=socket_address,
    #     reuse_port=True
    #     )
    # server_socket.accept()  # wait for client

    with socket.create_server(
            address=socket_address,
            reuse_port=True
            ) as server_socket:
        # Block until we receive an incoming connection
        connection, address = server_socket.accept()

        print(f"accepted connection from {socket_address}")

        # Read data
        # data: bytes = connection.recv(1024)

        _receive_client_input(sock_conn=connection)


if __name__ == "__main__":
    main()
