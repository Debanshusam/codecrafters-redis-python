import socket  # noqa: F401
# from loguru import logger


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


def _decode_resp_to_string(resp_string: bytes) -> str:
    string_data: str = resp_string.decode(encoding="utf-8")
    return string_data


def _ping_pong_implementation(data: bytes) -> bytes:
    return _encode_string_as_resp2(string="PONG")


def _receive_delimited_message(sock: socket.socket, delimiter=b'\n') -> None:
    data: bytes = b''
    while True:
        part = sock.recv(1024)
        data += part

        for each_cmd in data.split(delimiter):
            # Write the same data back
            respsonse_data: bytes = _redis_cmd_router(data=each_cmd)
            sock.sendall(respsonse_data)


def _redis_cmd_router(data: bytes) -> bytes:

    # PING-PONG cmd
    text_data: str = _decode_resp_to_string(resp_string=data)
    if "ping" in text_data.lower():
        print(f"PING Command detected: {text_data}")
        return _ping_pong_implementation(data=data)
    else:
        # returning back the input data
        return data


def main():
    # You can use print statements as follows for debugging, t
    # hey'll be visible when running tests.
    print("Logs from your program will appear here!")
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

        _receive_delimited_message(
            sock=connection,
            delimiter=b"\n"
        )


if __name__ == "__main__":
    main()
