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
        else:
            log = "at "

        if self.file:
            log += f"`{self.file}` "

        if self.node:
            log += f"in `${self.node.type}s.{self.node.name}` "

        if self.operation:
            log += f"operation `{self.operation.type}` "

        return "(" + log.strip() + ") "


    def __add__(self, other):
        return str(self) + other



class ErrorHandler:
    def __init__(self, file=None, line=None, node=None, operation=None):
        self.ctx = ErrorContext(file=file, line=line, node=node, operation=operation)

    def assert_key_exists(self, obj, key):
        if key not in obj.keys():
            raise Exception(
                self.ctx + f"must define `{key}`"
            )

    def assert_key_type_is(self, obj, key, desired_type):
        if not isinstance(obj[key], desired_type):
            _key_type = type(obj[key])
            raise Exception(
                self.ctx + f"`{key}` is defined, but wrong type (should be {desired_type}, is {_key_type})"
            )

    def assert_key_exists_and_type_is(self, obj, key, desired_type):
        self.assert_key_exists(obj, key)
        self.assert_key_type_is(obj, key, desired_type)

    def throw(self, message):
        raise Exception(self.ctx + message)
