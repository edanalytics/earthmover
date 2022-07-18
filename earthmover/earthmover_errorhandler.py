class ErrorContext:
    def __init__(self, file=None, line=None, node=None, operation=None):
        self.update(file=file, line=line, node=node, operation=operation)

    def update(self, file=None, line=None, node=None, operation=None):
        self.file = file
        self.line = line
        self.node = node
        self.operation = operation
    
    def add(self, file=None, line=None, node=None, operation=None):
        if file!=None: self.file = file
        if line!=None: self.line = line
        if node!=None: self.node = node
        if operation!=None: self.operation = operation

    def remove(self, *args):
        for arg in args:
            if arg=='file': self.file = None
            if arg=='line': self.line = None
            if arg=='node': self.node = None
            if arg=='operation': self.operation = None

    def __repr__(self):
        str = ""
        if self.line: str += "near line {0} of".format(self.line)
        if str!="": str += " "
        else: str += "at "
        if self.file: str += "{0}".format(self.file)
        if str!="": str += " "
        if self.node: str += "in ${0}s.{1}".format(self.node.type, self.node.name)
        if str!="": str += " "
        if self.operation: str += "operation `{0}`".format(self.operation["operation"])
        if str!="": return "(" + str.strip() + ") "
        else: return str
    
    def __add__(self, other):
        return str(self) + other


class ErrorHandler:
    def __init__(self, file=None, line=None, node=None, operation=None):
        self.ctx = ErrorContext(file=file, line=None, node=None, operation=None)
    
    def assert_key_exists(self, object, key):
        if key not in object.keys():
            raise Exception(self.ctx + "must define `{0}`".format(key))
    
    def assert_key_type_is(self, object, key, desired_type):
        if type(object[key])!=desired_type:
            if not (desired_type==dict and str(type(object[key]))=="<class 'earthmover.earthmover.dotdict'>"):
                raise Exception(self.ctx + "`{0}` is defined, but wrong type (should be {1}, is {2})".format(key, desired_type, type(object[key])))
    
    def assert_key_exists_and_type_is(self, object, key, desired_type):
        self.assert_key_exists(object, key)
        self.assert_key_type_is(object, key, desired_type)

    def throw(self, message):
        raise Exception(self.ctx + message)
