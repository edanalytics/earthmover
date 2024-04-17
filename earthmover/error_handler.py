from typing import Any, Optional

class ErrorContext:
    """

    """
    def __init__(self, file=None, line=None, node=None, operation=None):
        self.file = None
        self.line = None
        self.node = None
        self.operation = None

        self.update(file=file, line=line, node=node, operation=operation)


    def update(self, file=None, line=None, node=None, operation=None):
        self.file = file
        self.line = line
        self.node = node
        self.operation = operation


    def add(self, file=None, line=None, node=None, operation=None):
        if file:
            self.file = file
        if line:
            self.line = line
        if node:
            self.node = node
        if operation:
            self.operation = operation


    def remove(self, *args):
        for arg in args:
            if arg == 'file':
                self.file = None
            if arg == 'line':
                self.line = None
            if arg == 'node':
                self.node = None
            if arg == 'operation':
                self.operation = None


    def __repr__(self) -> str:
        """
        Example error messages:
            "(near line 79 of `config.yaml` in `$transformations.joined_inventories` operation `join`)
            "(at `$destinations.big_cats`)

        :return:
        """
        if self.line:
            log = f"near line {self.line} of "
        elif self.file:
            log = "at "
        else:
            log = ""

        if self.file:
            log += f"`{self.file}` "

        if self.node:
            log += f"in `{self.node.full_name}` "

        if self.operation:
            log += f"operation `{self.operation.type}` "

        if len(log.strip())>0:
            return "(" + log.strip() + ") "
        else:
            return ""


    def __add__(self, other):
        return str(self) + other



class ErrorHandler:
    def __init__(self, file=None, line=None, node=None, operation=None):
        self.ctx = ErrorContext(file=file, line=line, node=node, operation=operation)

    def assert_get_key(self, obj: dict, key: str,
        dtype: Optional[type] = None,
        required: bool = True,
        default: Optional[object] = None
    ) -> Any:
        """

        :param obj:
        :param key:
        :param dtype:
        :param required:
        :param default:
        :return:
        """
        value = obj.get(key)

        if value is None:
            if required:
                raise Exception(
                    f"{self.ctx} must define `{key}`"
                )
            else:
                return default

        if dtype and not isinstance(value, dtype):
            raise Exception(
                f"{self.ctx} `{key}` is defined, but wrong type (should be {dtype}, is {type(value)})"
            )

        return value

    def throw(self, message: str):
        raise Exception(
            f"{self.ctx} {message})"
        )
