import abc
import csv
import io
import json
import sys
import typing

import pydantic
import tabulate
import yaml

import logging


logger = logging.getLogger(__name__)

OutputFormat = typing.Literal["table", "csv", "yaml", "json"]

class PydanticBaseModel(pydantic.BaseModel, abc.ABC):

    @classmethod
    @abc.abstractmethod
    def table_header(cls) -> typing.List[str]:
        pass

    @abc.abstractmethod
    def to_table_row(self) -> typing.List[str]:
        pass

    @classmethod
    def csv_header(cls) -> typing.List[str]:
        return cls.table_header()

    def to_csv_row(self) -> typing.List[str]:
        return self.to_table_row()

    @classmethod
    def format_reprs(cls, reprs: typing.List['PydanticBaseModel'], fmt: OutputFormat = "table") -> str:
        if hasattr(cls, "sort_key"):
            reprs = sorted(reprs, key=lambda o: o.sort_key)

        if fmt == "yaml":
            class _ModelList(pydantic.RootModel):
                root: typing.List[cls]
            root = _ModelList(root=reprs)
            json_doc = root.model_dump_json(indent=2)
            doc = yaml.safe_load(json_doc)
            return yaml.safe_dump(doc, default_flow_style=False, sort_keys=False)
        elif fmt == "json":
            class _ModelList(pydantic.RootModel):
                root: typing.List[cls]
            root = _ModelList(root=reprs)
            return root.model_dump_json(indent=2)
        elif fmt == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(cls.csv_header())
            for l in reprs:
                writer.writerow(l.to_csv_row())
            return output.getvalue()
        else:
            return tabulate.tabulate([l.to_table_row() for l in reprs], headers=cls.table_header())

    @classmethod
    def format_repr(cls, r: 'PydanticBaseModel', fmt: OutputFormat = "table") -> str:
        if fmt == "yaml":
            json_doc = r.model_dump_json(indent=2)
            doc = yaml.safe_load(json_doc)
            return yaml.safe_dump(doc, default_flow_style=False)
        elif fmt == "json":
            return r.model_dump_json(indent=2)
        else:
            return cls.format_reprs([r], fmt)

def parse_reprs_file(
        cls: typing.Type[pydantic.BaseModel],
        filepath: typing.Union[str, typing.Literal["-"]],
        expect_list = True,
        fmt: typing.Optional[OutputFormat] = None
) -> typing.Tuple[typing.List['PydanticBaseModel'], typing.List[str]]:
    """
    Parse the file at filepath as either a CSV, YAML, or JSON. Input is expected to be a list.
    Each input is cast into cls (a pydantic model) and may raise a validation error.
    Collect a list of errors as strings into the return value.
    Don't return any objects if the parsing fails for even a single value.
    """
    reprs, errors = [], []
    if filepath == "-":
        content = sys.stdin.read()
        if not fmt:
            logger.warning("input format not specified, assuming JSON")
            fmt = "json"
    else:
        with open(filepath, "r") as f:
            content = f.read()

        # if we don't know the format, use the file extension to guess it
        allowed_fmts = ("csv", "yaml", "yml", "json")
        if not fmt or fmt not in allowed_fmts:
            ext = filepath.lower().rsplit(".", 1)[-1] if "." in filepath else ""
            if ext not in allowed_fmts:
                return [], [f"cannot parse: unknown file extension '{ext}', allowed: {', '.join(allowed_fmts)}"]

    try:
        if fmt == "csv":
            reader = csv.DictReader(io.StringIO(content))
            items = list(reader)
        elif fmt in ("yaml", "yml"):
            items = yaml.safe_load(content)
        else:
            items = json.loads(content)
    except Exception as e:
        errors.append(f"Failed to parse file {filepath} as {fmt}: {e}")
        return [], errors

    if not isinstance(items, list) and expect_list:
        errors.append(f"Input must be a list of {cls.__name__}, got {type(items).__name__}")
        return [], errors
    elif not expect_list:
        if not isinstance(items, dict):
            errors.append(f"Input must be a {cls.__name__}, got {type(items).__name__}")
            return [], errors
        else:
            items = [items]

    for i, obj in enumerate(items):
        try:
            r = cls(**obj)
            reprs.append(r)
        except Exception as e:
            name = obj.get("name", "<unknown>")
            errors.append(f"Error parsing object '{name}' at index {i}: {e}")

    if errors:
        return [], errors
    return reprs, []
