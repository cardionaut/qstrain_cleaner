import os
import pypdf
import re

import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path


class Cleaner:
    def __init__(self, root, out_path) -> None:
        self.root = root
        self.patients = [f.name for f in os.scandir(self.root) if f.is_dir()]
        self.out_file = pd.read_excel(out_path, sheet_name='SPSS Export (2)')

    def __call__(self) -> pd.DataFrame:
        for patient in self.patients:
            first_name, last_name = self.split_name(patient)
            logger.debug(f'{patient}: {first_name} _ {last_name}')
            self.read_pdf(patient, first_name, last_name)

        return self.out_file

    def split_name(self, patient):
        regex = re.compile(u'[^a-zA-Z\'-] + ')
        cleaned_patient = regex.sub('', patient)
        name_list = cleaned_patient.split(sep=' ')

        if len(name_list) == 2:
            first_name = name_list[1]
            last_name = name_list[0]
        elif (
            'von' in [name.lower() for name in name_list]
            or 'van' in [name.lower() for name in name_list]
            or 'le' in [name.lower() for name in name_list]
        ):
            first_name = ' '.join(name_list[2:])
            last_name = ' '.join(name_list[:2])
        else:
            first_name = ' '.join(name_list[1:])
            last_name = name_list[0]

        # check names in out_file
        

        return first_name, last_name

    def read_pdf(self, patient, first_name, last_name) -> None:
        """Read pdf report and store data in out_file"""
        if Path(os.path.join(self.root, patient, 'Results', 'Report.pdf')).is_file():
            reader = pypdf.PdfReader(os.path.join(self.root, patient, 'Results', 'Report.pdf'))
        elif Path(os.path.join(self.root, patient, 'results', 'Report.pdf')).is_file():
            reader = pypdf.PdfReader(os.path.join(self.root, patient, 'results', 'Report.pdf'))
        elif Path(os.path.join(self.root, patient, 'Results', 'Bericht.pdf')).is_file():
            reader = pypdf.PdfReader(os.path.join(self.root, patient, 'Results', 'Bericht.pdf'))
        elif Path(os.path.join(self.root, patient, 'results', 'Bericht.pdf')).is_file():
            reader = pypdf.PdfReader(os.path.join(self.root, patient, 'results', 'Bericht.pdf'))
        else:
            logger.info(f'No Report.pdf found for patient {patient}, skipping...')
            return

        text = reader.pages[0].extract_text().split(sep='\n')
