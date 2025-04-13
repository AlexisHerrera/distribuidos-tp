# Movie
# Rating
# Cast
# MovieSentiment
# EOF
# ACK
# ...
# Message
#   type = Movie, Cast, MovieSentiment, EOF, ...
#   data = list[Movie | Cast | MovieSentiment] | None
# Message.encode()
# Message.from_bytes(body)

from functools import singledispatchmethod
from typing import overload


class A:
    pass

class B:
    pass

type As = list[A]

class Message():
    @singledispatchmethod
    def from_data(self, data: str):
        print("default", data)

    @overload(As)
    def _(self, data: As):
        print("intsss", data)

    @overload(B)
    def _(self, data: B):
        print("str", data)

    def encode(self):
        pass
