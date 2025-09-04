Tests in this folder are expected to be run with an env that contains
all the required python deps (redfish, django, etc) that are not
part of monolith, hence they are seperate of the tests/ folder
and are not a module declared with `__init__.py` to prevent monolith's
python from importing their (non-existant) dependencies.