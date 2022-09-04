import re
import os
import jinja2
from jinja2 import Environment, FileSystemLoader
from earthmover.earthmover_node import Node

class Destination(Node):
    def __init__(self, name, config, loader):
        super().__init__(name, loader)
        self.type = "destination"
        self.meta = self.loader.to_dotdict(config)
        self.config = self.loader.to_dotdict(config)
        self.error_handler.ctx.update(file=self.loader.config_file, line=self.meta.__line__, node=self, operation=None)
        self.error_handler.assert_key_exists_and_type_is(self.meta, "source", str)
        self.source = self.meta.source
        self.error_handler.assert_key_exists_and_type_is(self.meta, "template", str)
        self.template = self.meta.template
        self.header = ""
        self.footer = ""
        self.mode = "w" # by default, (over)write files... but if source is chunked, need to append instead

    def do(self):
        self.error_handler.ctx.update(file=self.loader.config_file, line=self.config.__line__, node=self, operation=None)
        # load template
        exp = re.compile(r"\s+")
        try:
            file = open(self.template, 'r')
        except Exception as e:
            self.error_handler.throw("`template` file {0} cannot be opened ({1})".format(self.template, e))
        with file:
            template_string = file.read()
            # Allow people to create pretty templates (with spacing line breaks, etc.) but "linearize" them
            # prior to writing out to a jsonl file (so that each line is a record)... the following code
            # "linearizes" the JSON template (once), which is then (repeatedly) parsed by Jinja.
            if self.config.linearize:
                template_string = template_string.replace("\r\n", "")
                template_string = template_string.replace("\n", "")
                template_string = template_string.replace("\r", "")
                template_string = template_string.strip()
                template_string = exp.sub(" ", template_string).strip()
            try:
                template = Environment(
                                loader=FileSystemLoader(os.path.dirname('./'))
                                ).from_string(self.loader.config.macros + template_string)
            except Exception as e:
                self.loader.error_handler.throw("syntax error in Jinja template in `template` file {0} ({1})".format(self.template, e))
        # write output!
        target = self.loader.ref(self.source)
        self.rows += target.data.shape[0]
        self.cols = target.data.shape[1]
        target.data.fillna('', inplace=True)
        # assume that predecessor data is already loaded (this makes chunked processing work)
        file_name = os.path.join(self.loader.config.output_dir, f'{self.name}.{self.meta.extension}')
        file_path = os.path.dirname(file_name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        with open(file_name, self.mode) as file:
            for row in target.data.to_records(index=False):
                row_data = list(zip(target.meta["header_row"], row))
                try:
                    json_string = template.render(row_data)
                except Exception as e:
                    self.error_handler.throw("error rendering Jinja template in `template` file {0} ({1})".format(self.template, e))
                file.write(json_string + "\n")
        self.size = os.path.getsize(file_name)
        self.is_done = True
        self.logger.debug(f"output `{file_name}` written")
        self.loader.profile_memory()
    
    def wipe(self):
        try:
            file_name = os.path.join(self.loader.config.output_dir, f'{self.name}.{self.meta.extension}')
        except Exception as e:
            self.error_handler.ctx.update(file=self.loader.config_file, line=self.config.__line__, node=self, operation=None)
            self.error_handler.throw("error opening file {0} ({1})".format(file, e))
        with open(file_name, 'w') as file:
            file.write("")
    
    def clear(self):
        pass
