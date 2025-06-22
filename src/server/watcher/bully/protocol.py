MESSAGE_BYTES_AMOUNT = 1


class BullyProtocol:
    ALIVE = b'A'
    REPLY_ALIVE = b'R'
    ELECTION = b'E'
    ANSWER = b'W'
    COORDINATOR = b'C'
    TIMEOUT_REPLY = b'T'
