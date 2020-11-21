import traceback
import yaml
from pathlib import Path
import warnings
import py_idh.container as container

warnings.filterwarnings('ignore')

config_file_directory = Path(__file__).parent / "config.yaml"
try:
    with open(config_file_directory, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    for key, value in cfg.items():
        container.__dict__[key] = value
except:
    print(traceback.format_exc())
