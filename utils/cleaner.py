import os
import glob
import pypdf
import re
import lxml

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
            first_name, last_name, row = self.split_name(patient)
            row = self.read_pdf(patient, row)
            row = self.read_main(patient, row)

            self.out_file[(self.out_file['First_name'] == first_name) & (self.out_file['Name'] == last_name)] = row

        return self.out_file

    def split_name(self, patient):
        regex = re.compile(u"[^a-zA-Z'-] +")
        cleaned_patient = regex.sub(' ', patient).strip()
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
        row = self.out_file.query('Name == @last_name & First_name == @first_name').copy()
        try_counter = 0
        tmp_first, tmp_last = first_name, last_name
        while row.empty:  # name not found
            first_name, last_name = self.permute_name(tmp_first, tmp_last, try_counter)
            if tmp_first == first_name and tmp_last == last_name:  # all permutations tested
                break
            row = self.out_file.query('Name == @last_name & First_name == @first_name').copy()
            try_counter += 1

        if len(row.index) > 1:
            logger.warning(f'Non-unique patient {first_name} {last_name}.')

        return first_name, last_name, row

    def permute_name(self, first_name, last_name, counter):
        """Permute names until match is found in out_file"""
        if counter == 0:  # try using only first first_name
            first_name = first_name.split(sep=' ')[0]
        elif counter == 1:  # try using only second first_name
            first_name = first_name.split(sep=' ')[1]
        elif counter == 2:  # try using only first first_name (with -)
            first_name = first_name.split(sep='-')[0]
        elif counter == 3:  # try using only second first_name (with -)
            first_name = first_name.split(sep='-')[1]
        elif counter == 4:
            first_name = '-'.join(first_name.split(sep=' '))
        elif counter == 5:
            first_name = ' '.join(first_name.split(sep='-'))
        else:
            logger.warning(f'Patient {first_name} {last_name} not found.')

        return first_name, last_name

    def read_pdf(self, patient, row):
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
        # Extract values from pdf
        try:
            row['Date_CT'] = text[text.index('Report date/time:') + 1].split(' ')[0]
            row['weight_kg_report'] = text[text.index('Patient weight') + 1]
            row['hf_bpm_report'] = text[text.index('Heart rate') + 1]
            row['height_cm_report'] = text[text.index('Patient height') + 1]
            row['edmass_g_report'] = text[text.index('ED mass') + 1]
            row['edmass/bsa_g/m2_report'] = text[text.index('ED Mass/BSA') + 1]
        except ValueError:
            row['Date_CT'] = text[text.index('Datum/Uhrzeit des Berichts:') + 1].split(' ')[0]
            row['weight_kg_report'] = text[text.index('Gewicht des Patienten') + 1]
            row['hf_bpm_report'] = text[text.index('Herzfrequenz') + 1]
            row['height_cm_report'] = text[text.index('Größe des Patienten') + 1]
            row['edmass_g_report'] = text[text.index('ED Masse') + 1]
            row['edmass/bsa_g/m2_report'] = text[text.index('ED-Masse/BSA') + 1]

        row['bsa_m2_report'] = text[text.index('BSA') + 1]
        row['edv_ml_report'] = text[text.index('EDV') + 1]
        row['edv/bsa_ml/m2_report'] = text[text.index('EDV/BSA') + 1]

        return row

    def read_main(self, patient, row):
        """Read all (MAIN-..) files and store data in out-file"""
        ac_list = list(Path(self.root, patient).rglob('*(MAIN-a?c)*.txt'))
        if len(ac_list) != 1:
            logger.warning(f'LV LAX file not available or not unique for patient {patient}')
        else:
            ac_data = pd.read_csv(
                ac_list[0],
                sep='\t',
                header=None,
                names=['key', 'value', 'ignore_1', 'ignore_2'],
                on_bad_lines='skip',
                skip_blank_lines=True,
                engine='python',
            )
            ac_data = ac_data[['key', 'value']].dropna(how='any')
            index_of_interest = ac_data.index[ac_data['key'] == 'Average']
            

    def read_segmental(self, patient, row):
        pass
