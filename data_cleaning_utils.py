# data_cleaning_utils.py

import re

def clean_column_name(col_name):
    """
    Cleans a column name by removing leading/trailing whitespace,
    replacing non-breaking spaces, and collapsing multiple spaces.
    Ensures the column name is always a string.
    """
    if not isinstance(col_name, str):
        return str(col_name)

    cleaned_name = col_name.strip()
    cleaned_name = cleaned_name.replace('\xa0', ' ')
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
    return cleaned_name