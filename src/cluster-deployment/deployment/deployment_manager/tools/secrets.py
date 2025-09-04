import abc
from typing import Optional

from deployment_manager.common.models import SecretsProviderConfig

class SecretsProvider(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> Optional[str]:
        pass

class EmbeddedSecretsProvider(SecretsProvider):
    def __init__(self,  data: dict):
        self._data = data

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)


def create_secrets_provider(cfg: dict) -> SecretsProvider:
    """
    Args:
        cfg: profile config 'input.yml'
    """
    config = SecretsProviderConfig(**cfg.get("secrets_provider", {}))
    if config.provider == "embedded":
        return EmbeddedSecretsProvider(config.data)
    raise ValueError(f"unimplemented secrets provider '{config.provider}'")