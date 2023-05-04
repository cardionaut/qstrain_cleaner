import os
import hydra

import pandas as pd
from loguru import logger
from omegaconf import DictConfig, OmegaConf

from utils.cleaner import Cleaner

@hydra.main(version_base=None, config_path='.', config_name='config')
def main(config: DictConfig) -> None:
    root = config.root_dir
    in_path = config.in_path
    out_path = config.out_path
    out_path_cleaned = config.out_path_cleaned

    cleaner = Cleaner(root, in_path)
    new_data, new_data_cleaned = cleaner()

    with pd.ExcelWriter(out_path, datetime_format='DD/MM/YYYY') as writer:
        new_data.to_excel(writer, index=False)

    with pd.ExcelWriter(out_path_cleaned, datetime_format='DD/MM/YYYY') as writer:
        new_data_cleaned.to_excel(writer, index=False)

if __name__ == '__main__':
    main()