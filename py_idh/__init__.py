import traceback
import yaml
from pathlib import Path
import py_idh.container as container

config_file_directory = Path(__file__).parent / "config.yaml"
try:
    with open(config_file_directory, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    for key, value in cfg.items():
        container.__dict__[key] = value
except:
    print(traceback.format_exc())
