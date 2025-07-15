import os
import re

import pandas as pd
import unidecode

groups = pd.read_csv('grupos_pesquisa.csv')


def sanitize_filename(s):
    try:
        s = unidecode(s)
    except ImportError:
        pass
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'(^_|_$)', '', s)
    return s


def insert_researcher(): ...


for file in os.listdir('storage/research_groups'):
    if file.endswith('.csv'):
        print(file)
