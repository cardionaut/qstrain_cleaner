from typing import Any
import pandas as pd
import numpy as np
from loguru import logger


class Cleaner:
    def __init__(self, root, out_file) -> None:
        self.root = root
        self.out_file = out_file

    def __call__(self) -> pd.DataFrame:
        pass
