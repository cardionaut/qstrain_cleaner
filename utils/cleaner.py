import os
import pypdf
import re

import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path
import unicodedata


class Cleaner:
    def __init__(self, root, out_path) -> None:
        self.root = root
        self.patient_dirs = [f.name for f in os.scandir(self.root) if f.is_dir()]
        self.out_file = pd.read_excel(out_path, sheet_name='SPSS Export (2)')
        name_cols = ['Name', 'First_name']
        self.out_file[name_cols] = self.out_file[name_cols].apply(
            lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        )  # remove any umlauts and accents
        self.patients = [
            unicodedata.normalize('NFKD', pat).encode('ascii', errors='ignore').decode('utf-8')
            for pat in self.patient_dirs
        ]  # remove any umlauts and accents

    def __call__(self) -> pd.DataFrame:
        for patient, patient_dir in zip(self.patients, self.patient_dirs):
            file_list = list(Path(self.root, patient_dir).rglob('[R|r]esults'))
            if not file_list:
                logger.info(f'No Results directory found for patient {patient_dir}, skipping...')
                continue
            first_name, last_name, row = self.split_name(patient)
            row = self.read_pdf(patient_dir, row)
            row = self.read_main(patient_dir, row)

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
            first_name, last_name, failure = self.permute_name(tmp_first, tmp_last, try_counter)
            if failure:  # all name permutations tested, none fit
                break
            row = self.out_file.query('Name == @last_name & First_name == @first_name').copy()
            try_counter += 1
        if len(row.index) > 1:
            logger.warning(f'Non-unique patient {first_name} {last_name}.')

        return first_name, last_name, row

    def permute_name(self, first_name, last_name, counter):
        """Permute names until match is found in out_file"""
        failure = False
        if counter == 0:  # try using only first first_name
            first_name = first_name.split(sep=' ')[0]
        elif counter == 1:  # try using only second first_name
            try:
                first_name = first_name.split(sep=' ')[1]
            except IndexError:
                pass
        elif counter == 2:  # try using only first first_name (with -)
            first_name = first_name.split(sep='-')[0]
        elif counter == 3:  # try using only second first_name (with -)
            try:
                first_name = first_name.split(sep='-')[1]
            except IndexError:
                pass
        elif counter == 4:
            first_name = '-'.join(first_name.split(sep=' '))
        elif counter == 5:
            first_name = ' '.join(first_name.split(sep='-'))
        else:
            logger.warning(f'Patient {first_name} {last_name} not found.')
            failure = True

        return first_name, last_name, failure

    def read_pdf(self, patient_dir, row):
        """Read pdf report and store data in out_file"""
        file_list = list(Path(self.root, patient_dir).rglob('[R|r]esults/*t.pdf'))
        if not file_list:
            logger.info(f'No Report.pdf found for patient {patient_dir}, skipping...')
            return row

        reader = pypdf.PdfReader(file_list[0])
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

    def read_main(self, patient_dir, row):
        """Read all (MAIN-..) files and store data in out_file"""
        regexes = {
            'LV_LAX': '*(MAIN-a?c)*.txt',
            'LA': '*(MAIN-atrium)*.txt',
            'RV': '*(MAIN-rv)*.txt',
            'LV_SAX': '*(MAIN-*.txt',  # can have different names -> solve by excluding already read files later
        }  # regexes to find corresponding files
        keys = {
            'LV_LAX': 'EDV',
            'LA': 'EDV',
            'RV': 'EDA',
            'LV_SAX': 'EDA',
        }  # first key to search for in file
        n_keys = {
            'LV_LAX': 9,
            'LA': 7,
            'RV': 4,
            'LV_SAX': 9,
        }  # number of keys to read and copy
        start_cols = {
            'LV_LAX': 'LV_LAX_edv_ml',
            'LA': 'LA_edv_ml',
            'RV': 'RV_eda_cm2',
            'LV_SAX': 'LV_SAX_eda_cm2_average',
        }  # name of start col in out_file of each block 
        files_read = []

        for region in regexes.keys():
            file_list = list(Path(self.root, patient_dir).rglob(regexes[region]))
            file_list = [file for file in file_list if file not in files_read]  # for LV_SAX
            if not file_list:
                logger.warning(f'{region} file not available for patient {patient_dir}: {file_list}')
            else:
                files_read.append(file_list[0])
                region_data = pd.read_csv(
                    file_list[0],
                    sep='\t',
                    header=None,
                    names=['key', 'value', 'ignore_1', 'ignore_2'],
                    on_bad_lines='skip',
                    skip_blank_lines=True,
                    engine='python',
                )
                region_data = region_data[['key', 'value']].dropna(how='any').reset_index()
                index_of_interest = region_data.index[region_data['key'] == keys[region]][0]
                col_of_interest = row.columns.get_loc(start_cols[region])
                row.iloc[:, col_of_interest : col_of_interest + n_keys[region]] = region_data.iloc[
                    index_of_interest : index_of_interest + n_keys[region], 2
                ]

        return row

    def read_segmental(self, patient, row):
        pass
