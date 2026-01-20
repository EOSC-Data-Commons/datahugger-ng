import typing

CONSTANT: typing.Final = "FOO"

class Class:
    def __new__(cls, /, value: int) -> None: ...
    @property
    def value(self, /) -> int: ...

def list_of_int_identity(arg: list[int]) -> list[int]: ...
