import os
import hydra

import pandas as pd
from loguru import logger
from omegaconf import DictConfig, OmegaConf
from pathlib import Path

from utils.cleaner import Cleaner

@hydra.main(version_base=None, config_path='.', config_name='config')
def main(config: DictConfig) -> None:
    root = config.root_dir
    out_file = config.out_file

    cleaner = Cleaner(root, out_file)
    new_data = cleaner()

    with pd.ExcelWriter(out_file, datetime_format='DD/MM/YYYY') as writer:
        new_data.to_excel(writer, index=False)

if __name__ == '__main__':
    main()