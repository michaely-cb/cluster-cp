import logging
import pathlib
import string
from abc import ABCMeta, abstractmethod

from pkg_resources import resource_filename

logger = logging.getLogger(__name__)


class SwitchTemplateABC(metaclass=ABCMeta):

    @property
    @abstractmethod
    def dirname(self):
        """
        Return the name of the directory containing the template file
        """

    @property
    @abstractmethod
    def filename(self):
        """
        Return the name of the file containing the template
        """

    def __init__(self, vendor, model=None):
        template_path = None

        vendor_match = pathlib.Path(resource_filename(__name__, f"{self.dirname}/{vendor}/{self.filename}"))

        if model:
            model_match = pathlib.Path(resource_filename(__name__, f"{self.dirname}/{vendor}/{model}/{self.filename}"))
            if model_match.is_file():
                template_path = model_match
            elif vendor_match.parent.is_dir():
                # fuzzy match on model name - e.g. "Arista-7060x6-dcs-f" matches "7060X6"
                for model_dir in vendor_match.parent.iterdir():
                    if not model_dir.is_dir() or not model_dir.name.lower() in model.lower():
                        continue
                    fuzzy_model = model_dir.name
                    tp = pathlib.Path(resource_filename(__name__, f"{self.dirname}/{vendor}/{fuzzy_model}/{self.filename}"))
                    if tp.is_file():
                        logger.warning(
                            f"switch vendor={vendor} model={model} fuzzy match to {fuzzy_model} for ZTP template"
                        )
                        template_path = tp
                        break

            if not template_path:
                logger.warning(f"switch vendor={vendor} model={model} does not have a ZTP template file, searching for vendor default")

        if not template_path and vendor_match.is_file():
            template_path = vendor_match

        if not template_path:
            raise ValueError(f"switch vendor={vendor} model={model} filename={self.filename} does not have a ZTP template file")

        self.template_path: pathlib.Path = template_path
        self.template: string.Template = string.Template(template_path.read_text())


    def substitute(self, mapping=None, **kwargs):
        """
        Place values in template
        """
        if mapping is None:
            mapping = {}
        return self.template.substitute(mapping, **kwargs)


class MgmtSwitchZTPTemplate(SwitchTemplateABC):
    dirname = './data'
    filename = 'mgmt_switch_ztp_template.txt'


class MgmtSwitchStartupConfigTemplate(SwitchTemplateABC):
    dirname = './data'
    filename = 'mgmt_switch_startup_template.txt'


class DataSwitchZTPTemplate(SwitchTemplateABC):
    dirname = './data'
    filename = 'data_switch_ztp_template.txt'


class DataSwitchStartupConfigTemplate(SwitchTemplateABC):
    dirname = './data'
    filename = 'data_switch_startup_template.txt'
