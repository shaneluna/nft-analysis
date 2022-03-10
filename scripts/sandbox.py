# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: nft-analysis-venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Imports

# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
import sqlite3

# %% [markdown]
# ## Read Data

# %%
con = sqlite3.connect("../data/nfts.sqlite")
cur = con.cursor()

# %% [markdown]
# ## List All Available Tables

# %%
query = cur.execute("""SELECT * 
FROM sqlite_master 
WHERE type='table'""")
cols = [column[0] for column in query.description]
results = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
results

# %% [markdown]
# ## Union All Transactions

# %%
# mints: 6667282
# transfers: 4514729
# combined: 11182011
query = cur.execute("""
SELECT *, 'mint' AS activity_type
FROM mints
""")
cols = [column[0] for column in query.description]
mints_df = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
mints_df.head()

# %%
query = cur.execute("""
SELECT *, 'transfer' as activity_type
FROM transfers
""")
cols = [column[0] for column in query.description]
transfers_df = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
transfers_df.head()

# %%
transactions_df = pd.concat([mints_df, transfers_df])
transactions_df.shape

# %%
transactions_df = transactions_df.sort_values(by=['timestamp'], ascending=[True])
transactions_df.head()

# %%
transactions_df['token_transaction_order'] = transactions_df.groupby(['nft_address','token_id']).cumcount()+1

# %%
transactions_df

# %%
transactions_df[transactions_df["transaction_value"] == 0.0]


# %% [markdown]
# # Questions

# %%
def get_results(cur: sqlite3.Cursor = cur, statement: str = '') -> pd.DataFrame:
    '''
    Returns results from sqlite query, in the form of a pandas dataframe.
    '''
    query = cur.execute(statement)
    cols = [col[0] for col in query.description]
    df = pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
    return df


# %%
# who has made the most money (takes 3min 16sec)
statement = '''
with
    mints_and_transfers as (
        select
            *
            , 'mint' as activity
        from mints
        union all
        select
            *
            , 'transfer' as activity
        from transfers
    )
    , nfts_with_order as (
        select
            transaction_hash
            , nft_address
            , token_id
            , activity
            , from_address
            , to_address
            , transaction_value
            , timestamp
            , row_number() over (
                partition by nft_address, token_id
                order by timestamp asc
            ) as row_num
        from mints_and_transfers
    )
    , starts_join_ends as (
        select
            nwo1.transaction_hash
            , nwo1.nft_address
            , nwo1.token_id
            , nwo1.activity as start_activity
            , nwo1.to_address as start_address
            , nwo1.transaction_value as start_value
            , nwo1.timestamp as start_timestamp
            , nwo1.row_num as start_row
            , ifnull(nwo2.activity, 'hold') as end_activity
            , ifnull(nwo2.to_address, nwo1.to_address) as end_address
            , ifnull(nwo2.transaction_value, cmv.market_value) as end_value
            , ifnull(nwo2.timestamp, 1632586540) as end_timestamp
            , ifnull(nwo2.row_num, nwo1.row_num) as end_row
        from nfts_with_order as nwo1
        left join nfts_with_order as nwo2 on 
            nwo1.nft_address = nwo2.nft_address
            and nwo1.token_id = nwo2.token_id
            and nwo1.row_num + 1 = nwo2.row_num
        left join current_market_values as cmv on
            nwo1.nft_address = cmv.nft_address
            and nwo1.token_id = cmv.token_id
    )
    , pairs_with_deltas as (
        select
            *
            , end_value - start_value as delta_value
            , julianday(end_timestamp, 'unixepoch') - julianday(start_timestamp, 'unixepoch') as delta_days
            , cast(start_row as string) || '->' || cast(end_row as string) as row_change
        from starts_join_ends
    )
select * from pairs_with_deltas
'''

df = get_results(cur, statement)
pickle.dump(df, open('../pickles/df_q1.pkl', 'wb'))
df.head()

# %%

# %%
