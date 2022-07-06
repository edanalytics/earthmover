import re
import os
import csv
import time
import jinja2
from jinja2 import Environment, FileSystemLoader
import numpy as np
import networkx as nx
import pandas as pd
from datetime import datetime
from earthmover.earthmover_node import Node

def shuffle_df(df):
    df = df.copy().reset_index(drop=True)
    for col in df.columns.values:
        df[col] = df[col].sample(frac=1).reset_index(drop=True)
    return df


class Transformation(Node):
    def __init__(self, name, ops, loader):
        super().__init__(name, loader)
        self.type = "transformation"
        self.operations = ops
        self.meta = self.loader.to_dotdict({})
        self.error_handler = self.loader.error_handler

    def do(self):
        if not self.is_done:
            transformed = None
            for op in self.operations:
                op = self.loader.to_dotdict(op)
                self.error_handler.ctx.update(file=self.loader.config_file, line=op["__line__"], node=self, operation=op)
                self.error_handler.assert_key_exists_and_type_is(op, "operation", str)
                if   op.operation=="join":                transformed = self.do_join(op)
                elif op.operation=="union":               transformed = self.do_union(op)
                elif op.operation=="add_columns":         transformed = self.do_add_columns(op)
                elif op.operation=="modify_columns":      transformed = self.do_modify_columns(op)
                elif op.operation=="duplicate_columns":   transformed = self.do_duplicate_columns(op)
                elif op.operation=="rename_columns":      transformed = self.do_rename_columns(op)
                elif op.operation=="drop_columns":        transformed = self.do_mod_columns(op, 'drop')
                elif op.operation=="keep_columns":        transformed = self.do_mod_columns(op, 'keep')
                elif op.operation=="combine_columns":     transformed = self.do_combine_columns(op)
                elif op.operation=="distinct_rows":       transformed = self.do_distinct_rows(op)
                elif op.operation=="map_values":          transformed = self.do_map_values(op)
                elif op.operation=="filter_rows":         transformed = self.do_filter_rows(op)
                elif op.operation=="date_format":         transformed = self.do_date_format(op)
                elif op.operation=="group_by_with_count": transformed = self.do_group_by_with_count(op)
                elif op.operation=="group_by_with_agg":   transformed = self.do_group_by_with_agg(op)
                elif op.operation=="group_by":            transformed = self.do_group_by(op)
                else:
                    self.error_handler.throw("invalid transformation operation `{0}`".format(op.operation))
                self.loader.profile("   transformation {0} {1} processed".format(self.name, op.operation))
                #self.is_done = True
                self.data = transformed.copy()
                if "debug" in op.keys() and op.debug:
                    print("DEBUG: `{0}` transformation on ${1}s.{2} completed".format(op.operation, self.type, self.name))
                    print(f"       transformed shape: {transformed.shape[0]} rows, {transformed.shape[1]} cols")
                    print("       transformed columns: ", transformed.columns.values)
                    # print("       RANDOMLY-SHUFFLED PREVIEW:")
                    # print(shuffle_df(transformed).head(5))
                    print(transformed)
            self.rows += transformed.shape[0]
            self.cols = transformed.shape[1]
            self.meta.header_row = self.data.columns.values.tolist()
            self.meta.num_rows = len(self.data)
            self.memory_usage = self.data.memory_usage(deep=True).sum()
            self.age = time.time()
            self.loader.profile_memory()
            self.is_done = True

    def load_op_source_data(self, op, index=0):
        if "source" in op.keys():
            source = self.loader.ref(op.source)
        elif "sources" in op.keys():
            try:
                source = self.loader.ref(op.sources[index])
            except IndexError:
                self.error_handler.throw("Too few sources defined")
        return source.data
    
    def has_chunked_ancestor(self, node_name):
        node = self.loader.lookup_node(node_name)
        has_chunked = False
        if node.type!="source":
            ancestors = nx.ancestors(self.loader.graph, node_name)
            ancestor_nodes = [node for node in self.loader.graph.nodes(data=True) if node[0] in ancestors]
            origins = [node[1]["data"] for node in ancestor_nodes if node[1]["data"].type=="source"]
            for origin in origins:
                if origin.is_chunked: has_chunked = True
        elif node.is_chunked: has_chunked = True
        return has_chunked

    def do_join(self, op):
        source1_df = self.load_op_source_data(op, 0)
        source2_df = self.load_op_source_data(op, 1)
        if "left_key" not in op.keys() and "left_keys" not in op.keys():
            self.error_handler.throw("must define `left_key` or `left_keys`")
        elif "left_key" in op.keys() and isinstance(op.left_key, str):
            left_on = []
            left_on.append(op.left_key)
        elif "left_keys" in op.keys() and isinstance(op.left_keys, list):
            left_on = op.left_keys
        else:
            self.error_handler.throw("`left_key(s)` incorrectly specified (should be string or list of strings)")
        
        if "right_key" not in op.keys() and "right_keys" not in op.keys():
            self.error_handler.throw("must define `right_key` or `right_keys`")
        elif "right_key" in op.keys() and isinstance(op.right_key, str):
            right_on = []
            right_on.append(op.right_key)
        elif "right_keys" in op.keys() and isinstance(op.right_keys, list):
            right_on = op.right_keys
        else:
            self.error_handler.throw("`right_key(s)` incorrectly specified (should be string or list of strings)")
        
        if "join_type" not in op.keys():
            self.error_handler.throw("must define `join_type`")
        if op.join_type not in ["inner", "left", "right", "outer"]:
            self.error_handler.throw("`join_type` must be one of [inner, left, right, outer], not `"+op.join_type + "`")
        join_type = op.join_type

        # check that chunked joins will work:
        left_has_chunked = self.has_chunked_ancestor(op.sources[0])
        right_has_chunked = self.has_chunked_ancestor(op.sources[1])
        if join_type=="left":
            if right_has_chunked:
                self.error_handler.throw("left `join` operation is incompatible with a large (chunked) source on the right")
        if join_type=="right":
            if left_has_chunked:
                self.error_handler.throw("right `join` operation is incompatible with a large (chunked) source on the left")
        if join_type=="inner":
            if left_has_chunked or right_has_chunked:
                self.error_handler.throw("inner `join` operation is incompatible with a large (chunked) source on either side")

        if "left_keep_columns" in op.keys() and isinstance(op.left_keep_columns, list):
            for k in left_on:
                if k not in op.left_keep_columns:
                    op.left_keep_columns.append(k)
            source1_df = source1_df[op.left_keep_columns]
        elif "left_drop_columns" in op.keys() and isinstance(op.left_drop_columns, list):
            for k in left_on:
                if k in op.left_drop_columns:
                    self.loader.error_handler.throw("you may not `left_drop_columns` that are part of the `left_key(s)`")
            source1_df = source1_df.drop(columns=op.left_drop_columns)
        if "right_keep_columns" in op.keys() and isinstance(op.right_keep_columns, list):
            for k in right_on:
                if k not in op.right_keep_columns:
                    op.right_keep_columns.append(k)
            source2_df = source2_df[op.right_keep_columns]
        elif "right_drop_columns" in op.keys() and isinstance(op.right_drop_columns, list):
            for k in right_on:
                if k in op.right_drop_columns:
                    self.error_handler.throw("you may not `right_drop_columns` that are part of the `right_key(s)`")
            source2_df = source2_df.drop(columns=op.right_drop_columns)
        
        try:
            transformed = pd.merge(source1_df, source2_df, how=join_type, left_on=left_on, right_on=right_on)
        except Exception as e:
            self.error_handler.throw("Error during `join` operation. Check your join keys?")
        return transformed

    def do_union(self, op):
        num_sources = len(op.sources)
        chunked_index = [0] * num_sources
        done_index = [0] * num_sources
        for i in range(0, len(op.sources)):
            if self.has_chunked_ancestor(op.sources[i]):
                chunked_index[i] = 1
            else:
                source = self.loader.lookup_node(op.sources[i])
                if source.is_done:
                    done_index[i] = 1
        if sum(chunked_index) > 1:
            self.error_handler.throw("`union` operation can have at most one large (chunked) source")
        elif sum(chunked_index)==1 and sum(done_index)==num_sources-1: # all non-chunked sources are done
            chunked = chunked_index.index(1)
            df = self.load_op_source_data(op, chunked)
            return df
        elif sum(chunked_index)==1: # need to concat non-chunked sources with first chunk of chunked sources
            chunked = chunked_index.index(1)
            transformed = self.load_op_source_data(op, chunked)
            for i in range(0, len(op.sources)):
                if i==chunked: continue
                source_df = self.load_op_source_data(op, i)
                try:
                    transformed = pd.concat([transformed, source_df], ignore_index=True)
                except Exception as e:
                    self.error_handler.throw("Error during `union` operation. Are sources same shape?")
        else: # no chunked sources!
            transformed = self.load_op_source_data(op, 0)
            for i in range(1, len(op.sources)):
                source_df = self.load_op_source_data(op, i)
                try:
                    transformed = pd.concat([transformed, source_df], ignore_index=True)
                except Exception as e:
                    self.error_handler.throw("Error during `union` operation. Are sources same shape?")
        return transformed

    def do_rename_columns(self, op):
        source_df = self.load_op_source_data(op)
        self.loader.error_handler.assert_key_exists_and_type_is(op, "columns", dict)
        return source_df.rename(columns=dict(op.columns))

    def do_add_columns(self, op):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "columns", dict)
        for col, val in op.columns.items():
            if col=="__line__": continue
            if isinstance(val, str) and "{{" in val and "}}" in val:
                try:
                    # template = jinja2.Template(self.loader.config.macros + val)
                    template = Environment(
                                loader=FileSystemLoader(os.path.dirname('./'))
                                ).from_string(self.loader.config.macros + val)
                except Exception as e:
                    self.error_handler.throw("Syntax error in Jinja template for column `{0}` of `add_columns` operation ({1})".format(col, e))
                source_df = source_df.apply(self.apply_jinja, axis=1, args=(template, col, 'add'))
            else:
                source_df[col] = val
        return source_df
    
    def do_modify_columns(self, op):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "columns", dict)
        for col, val in op.columns.items():
            if col=="__line__": continue
            if isinstance(val, str) and (("{{" in val and "}}" in val) or ("{%" in val and "%}" in val)):
                try:
                    # template = jinja2.Template(self.loader.config.macros + val)
                    template = Environment(
                                loader=FileSystemLoader(os.path.dirname('./'))
                                ).from_string(self.loader.config.macros + val)
                except Exception as e:
                    self.error_handler.throw("Syntax error in Jinja template for column `{0}` of `modify_columns` operation ({1})".format(col, e))
                source_df = source_df.apply(self.apply_jinja, axis=1, args=(template, col, 'modify'))
            else:
                source_df[col] = val
        return source_df
    
    def do_duplicate_columns(self, op):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "columns", dict)
        for old_col, new_col in op.columns.items():
            if old_col=="__line__": continue
            source_df[new_col] = source_df[old_col]
        return source_df

    def do_mod_columns(self, op, how="drop"):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "columns", list)
        if how=="keep":
            for col in [set(source_df.columns.values) - set(op.columns)]:
                source_df = source_df.drop(columns=col)
        elif how=="drop":
            for col in op.columns:
                source_df = source_df.drop(columns=col)
        return source_df

    def do_combine_columns(self, op):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "columns", list)
        self.error_handler.assert_key_exists_and_type_is(op, "new_column", str)
        sep = ""
        if "separator" in op.keys(): sep = op["separator"]
        source_df[op.new_column] = [sep.join(row) for row in source_df[op.columns].values]
        return source_df

    def do_distinct_rows(self, op):
        if self.has_chunked_ancestor(op["source"]):
            self.error_handler.throw("A `distinct_rows` operation cannot run on a chunked (large) source.")
        source_df = self.load_op_source_data(op)
        old_len = len(source_df)
        if "columns" in op.keys():
            dup_cols = op.columns
            transformed = source_df.drop_duplicates(subset=dup_cols)
        else:
            self.loader.profile(" (`distinct_rows` operation using all columns for distinctness, since no `columns` were specified)")
            transformed = source_df.drop_duplicates()
        new_len = len(transformed)
        if old_len!=new_len and self.loader.config.verbose:
            self.loader.profile(" (`distinct_rows` operation removed {0} duplicate rows)".format(old_len-new_len))
        return transformed

    def do_filter_rows(self, op):
        source_df = self.load_op_source_data(op)
        old_len = len(source_df)
        self.error_handler.assert_key_exists_and_type_is(op, "query", str)
        query = op.query
        self.error_handler.assert_key_exists_and_type_is(op, "behavior", str)
        if op.behavior not in ["include", "exclude"]:
            self.error_handler.throw("`behavior` must be one of [include, exclude]")
        if op.behavior=="exclude": query = "not("+query+")"
        try:
            transformed = source_df.query(query)
        except Exception as e:
            self.error_handler.throw("Error during `filter_rows` operation. Check query format?")
        new_len = len(transformed)
        if old_len!=new_len and self.loader.config.verbose:
            self.loader.profile(" (`filter_rows` operation removed {0} rows)".format(old_len-new_len))
        return transformed

    def do_map_values(self, op):
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "column", str)
        if "column" in op.keys() and isinstance(op.column, str):
            columns = []
            columns.append(op.column)
        elif  "columns" and isinstance(op.columns, list):
            columns = op.columns
        # column_name = op.column
        if "mapping" in op.keys():
            self.error_handler.assert_key_type_is(op, "mapping", dict)
            mapping = op.mapping
            if "__line__" in op.mapping.keys():
                del mapping["__line__"]
        elif "map_file" in op.keys():
            self.error_handler.assert_key_type_is(op, "map_file", str)
            with open(op.map_file, 'r') as file:
                next(file) # skip first header row (we assume that map_files have a header)
                sep = self.loader.get_sep(op.map_file)
                try:
                    reader = csv.reader(file, delimiter=sep)
                except Exception as e:
                    self.error_handler.throw("error reading `map_file` {0}".format(file))
                mapping = dict(reader)
        else:
            self.error_handler.throw("must define either `mapping` (list of old_value: new_value) or a `map_file` (two-column CSV or TSV)")
        try:
            for column_name in columns:
                source_df[column_name] = source_df[column_name].replace(mapping)
        except Exception as e:
            self.error_handler.throw("Error during `map_values` operation. Check mapping shape and `column(s)`?")
        return source_df

    def do_date_format(self, op):
        source_df = self.load_op_source_data(op)
        if "column" in op.keys() and isinstance(op.column, str):
            columns = []
            columns.append(op.column)
        elif  "columns" and isinstance(op.columns, list):
            columns = op.columns
        else:
            self.error_handler.throw("a `date_format` operation must specify either one `column` or several `columns` to convert")

        for column in columns:
            try:
                source_df[column] = [ datetime.strptime(value, op.from_format).strftime(op.to_format) for value in source_df[column] ]
            except Exception as e:
                self.error_handler.throw("Error during `date_format` operation, `{0}` column. Check format strings? ({1})".format(column, e))
        return source_df
    
    def do_group_by_with_count(self, op):
        if self.has_chunked_ancestor(op["source"]):
            self.error_handler.throw("A `group_by_with_count` operation cannot run on a chunked (large) source.")
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "group_by_columns", list)
        self.error_handler.assert_key_exists_and_type_is(op, "count_column", str)
        col_sep = "_____"
        source_df["___grouped_col___"] = [col_sep.join(row) for row in source_df[op.group_by_columns].values]
        source_df = source_df.groupby("___grouped_col___", sort=False)
        source_df = source_df.size()
        source_df= source_df.reset_index(name=op.count_column)
        source_df[op.group_by_columns] = source_df["___grouped_col___"].str.split(col_sep, n=len(op.group_by_columns), expand=True)
        del source_df["___grouped_col___"]
        return source_df

    def do_group_by_with_agg(self, op):
        if self.has_chunked_ancestor(op["source"]):
            self.error_handler.throw("A `group_by_with_agg` operation cannot run on a chunked (large) source.")
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "group_by_columns", list)
        self.error_handler.assert_key_exists_and_type_is(op, "agg_column", str)
        col_sep = "_____"
        source_df["___grouped_col___"] = [col_sep.join(row) for row in source_df[op.group_by_columns].values]
        sep = ","
        if "separator" in op.keys() and isinstance(op.separator, str):
            sep = op.separator
        source_df = source_df.groupby("___grouped_col___", sort=False)
        source_df = source_df[[op.agg_column]].agg(lambda x: sep.join(x))
        source_df = source_df.reset_index()
        source_df[op.group_by_columns] = source_df["___grouped_col___"].str.split(col_sep, n=len(op.group_by_columns), expand=True)
        del source_df["___grouped_col___"]
        return source_df
    
    def do_group_by(self, op):
        if self.has_chunked_ancestor(op["source"]):
            self.error_handler.throw("A `group_by` operation cannot run on a chunked (large) source.")
        source_df = self.load_op_source_data(op)
        self.error_handler.assert_key_exists_and_type_is(op, "group_by_columns", list)
        self.error_handler.assert_key_exists_and_type_is(op, "create_columns", dict)
        
        grouped_df = source_df.groupby(op.group_by_columns)
        new_df = grouped_df.size().reset_index()
        new_column_names = op.group_by_columns + ["__GROUP_SIZE__"]
        new_df.columns = new_column_names
        
        for new_col_name, func in op.create_columns.items():
            if new_col_name=="__line__": continue
            # This matches column names with upper- and lower-case letters, numbers, and underscores. Enough?
            pieces = re.findall("([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)", func)[0]
            function = pieces[0]
            column = ""
            separator = ""
            if len(pieces)>1: column = pieces[1]
            if len(pieces)>2: separator = pieces[2]
            
            # check for errors:
            if function in ["agg", "aggregate", "max", "maximum", "min", "minimum", "sum", "mean", "avg", "std", "stdev", "stddev", "var", "variance"]:
                if column=="":
                    self.error_handler.throw("Aggregation function `{0}`(column) missing required column".format(function))
                if column not in source_df.columns.values:
                    self.error_handler.throw("Aggregation function `{0}`({1}) refers to a column {2} which does not exist".format(function, column, column))

            if function=='count' or function=='size':
                new_series = grouped_df.apply(lambda x: len(x))
            elif function=="agg" or function=="aggregate":
                new_series = grouped_df.apply(lambda x: separator.join(x[column]))
            elif function=="max" or function=="maximum":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).max())
            elif function=="min" or function=="minimum":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).min())
            elif function=="sum":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).sum())
            elif function=="mean" or function=="avg":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).sum()/max(1,len(x)))
            elif function=="std" or function=="stdev" or function=="stddev":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).std())
            elif function=="var"or function=="variance":
                new_series = grouped_df.apply(lambda x: pd.to_numeric(x[column]).var())
            else:
                self.error_handler.throw("Invalid aggregation function `{0}` in `group_by` operation".format(function))

            computed = new_series.to_frame(new_col_name).reset_index()
            new_df = new_df.merge(computed, how="left", on=op.group_by_columns)

        new_df = new_df.query("__GROUP_SIZE__ > 0")
        del new_df["__GROUP_SIZE__"]
        return new_df

    def clear(self):
        self.meta = {}
        self.data = None
        self.memory_usage = 0
        self.age = 0