from deployment_manager.common import models


def test_default_model_reflexive():
    cfg_base0 = models.ClusterDeploymentConfig()
    cfg_doc = cfg_base0.model_dump_json()
    cfg_base1 = models.ClusterDeploymentConfig.model_validate_json(cfg_doc)
    assert cfg_base0 == cfg_base1
