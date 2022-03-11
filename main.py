# module imports
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import pandas as pd
import pickle
import sqlite3

# %%
# sqlite connection
con = sqlite3.connect('../data/nfts.sqlite')
cur = con.cursor()

# %%
# function to query sqlite database
def get_results(cur: sqlite3.Cursor = cur, statement: str = '') -> pd.DataFrame:
    '''
    Returns results from sqlite query, in the form of a pandas dataframe.
    '''
    query = cur.execute(statement)
    cols = [col[0] for col in query.description]
    df = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
    return df

# %%
# building an initial dataset