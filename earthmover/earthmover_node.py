import jinja2


# superclass of Source, Transformation, Destination
class Node:
    def __init__(self, name, loader):
        self.type = ""
        self.name = name
        self.loader = loader
        self.logger = loader.logger
        self.error_handler = loader.error_handler
        self.memory_usage = 0
        self.age = 0
        self.is_chunked = False
        self.has_more = False
        self.is_done = False
        self.data = None
        self.rows = 0
        self.cols = 0
        self.size = 0
        self.expectations = []
    
    def check_expectations(self):
        if not self.is_done:
            self.logger.debug("skipping checking expectations (not yet loaded)")
            return
        if len(self.expectations) > 0:
            for expectation in self.expectations:
                template = jinja2.Template("{{" + expectation + "}}")
                result = self.data.apply(self.apply_jinja, axis=1, args=(template, "__expectation_result__", "expectations"))
                num_failed = len(result.query("__expectation_result__=='False'"))
                if num_failed > 0:
                    self.error_handler.throw("Source `${0}s.{1}` failed expectation `{2}` ({3} rows fail)".format(self.type, self.name, expectation, num_failed))
            result.drop(columns="__expectation_result__")

    def apply_jinja(self, row, template, col, func):
        if not self.is_chunked: row["___row_id___"] = row.name
        if func=="modify": row["value"] = row[col]
        try:
            value = template.render(row)
        except Exception as e:
            self.error_handler.throw("Error rendering Jinja template for column `{0}` of `{1}_columns` operation ({2})".format(col, func, e))
        row[col] = value
        if not self.is_chunked: del row["___row_id___"]
        if func=="modify": del row["value"]
        return row
