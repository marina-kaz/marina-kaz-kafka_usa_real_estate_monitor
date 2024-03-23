import pandas as pd
from constants import DATA_PATH
from typing import TypedDict


def load_data():
    return pd.read_csv(DATA_PATH, index_col=False)


class Sample(TypedDict):
    bed: float
    bath: float
    house_size: float
