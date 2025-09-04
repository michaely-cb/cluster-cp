import pytest
from common_init import current_dir, ClusterInfoObj

@pytest.mark.parametrize("ssh_host", ClusterInfoObj.get_all_usernodes())
def test_model_zoo_setup(client, ssh_host):
    print(client, ssh_host)