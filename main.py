# %%
# module imports
import argparse
import gc
import math
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import pandas as pd
import pickle
import sqlite3
import streamlit as st

# %%
# parsing arguments for use internally
parser = argparse.ArgumentParser()
parser.add_argument('--output_type', choices = ['plotly', 'streamlit'], default = 'streamlit')
args = parser.parse_args()

# %%
# sqlite connection
con = sqlite3.connect('data/nfts.sqlite')
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
# building an initial dataset (takes 3min 16sec on macbook m1 10 core cpu)
# who has made the most money 
# statement = '''
# with
#     mints_and_transfers as (
#         select
#             *
#             , 'mint' as activity
#         from mints
#         union all
#         select
#             *
#             , 'transfer' as activity
#         from transfers
#     )
#     , nfts_with_order as (
#         select
#             transaction_hash
#             , nft_address
#             , token_id
#             , activity
#             , from_address
#             , to_address
#             , transaction_value
#             , timestamp
#             , row_number() over (
#                 partition by nft_address, token_id
#                 order by timestamp asc
#             ) as row_num
#         from mints_and_transfers
#     )
#     , starts_join_ends as (
#         select
#             nwo1.transaction_hash
#             , nwo1.nft_address
#             , nwo1.token_id
#             , nwo1.activity as start_activity
#             , nwo1.to_address as start_address
#             , nwo1.transaction_value as start_value
#             , nwo1.timestamp as start_timestamp
#             , nwo1.row_num as start_row
#             , ifnull(nwo2.activity, 'hold') as end_activity
#             , ifnull(nwo2.to_address, nwo1.to_address) as end_address
#             , ifnull(nwo2.transaction_value, cmv.market_value) as end_value
#             , ifnull(nwo2.timestamp, 1632586540) as end_timestamp
#             , ifnull(nwo2.row_num, nwo1.row_num) as end_row
#         from nfts_with_order as nwo1
#         left join nfts_with_order as nwo2 on 
#             nwo1.nft_address = nwo2.nft_address
#             and nwo1.token_id = nwo2.token_id
#             and nwo1.row_num + 1 = nwo2.row_num
#         left join current_market_values as cmv on
#             nwo1.nft_address = cmv.nft_address
#             and nwo1.token_id = cmv.token_id
#     )
#     , pairs_with_deltas as (
#         select
#             *
#             , end_value - start_value as delta_value
#             , julianday(end_timestamp, 'unixepoch') - julianday(start_timestamp, 'unixepoch') as delta_days
#             , cast(start_row as string) || '->' || cast(end_row as string) as row_change
#         from starts_join_ends
#     )
# select * from pairs_with_deltas
# '''
# df = get_results(cur, statement)
df = pickle.load(open('pickles/df.pkl', 'rb'))

# %%
# finding profits per address
profit_df = df.query('end_activity != "hold"').copy()
profit_df = profit_df[['start_address', 'delta_value']].groupby(['start_address']).sum()
profit_df = profit_df.sort_values(by = ['delta_value'], ascending = [False])
profit_df.columns = ['total_wei_value']

# %%
# identifying top X winners and losers
top_X = 300

winners_df = profit_df.head(top_X).copy()
winners_df['total_eth_value'] = (winners_df['total_wei_value'] / 1000000000000000000)
winners_df['w_or_l'] = ['winner']*top_X

losers_df = profit_df.tail(top_X).copy()
losers_df['total_eth_value'] = (losers_df['total_wei_value'] / 1000000000000000000)
losers_df['w_or_l'] = ['loser']*top_X

del profit_df
gc.collect()

# %%
# building dataset of top and bottom X performers
cols = ['start_address', 'start_activity', 'end_activity', 'end_address', 'delta_value', 'delta_days']
final_df = winners_df[['w_or_l']].merge(df[cols], left_index = True, right_on = 'start_address')
final_df = pd.concat([
    final_df,
    losers_df[['w_or_l']].merge(df[cols], left_index = True, right_on = 'start_address')
])
final_df['delta_eth'] = final_df['delta_value']/1000000000000000000

del winners_df, losers_df
gc.collect()

# %%
# creating a displot for 'when / how to buy'
hist = px.histogram(
    final_df,
    x = 'start_activity',
    y = 'delta_eth',
    color = 'w_or_l',
    hover_data = final_df.columns,
    category_orders = {'start_activity': ['mint', 'transfer']},
    labels = {
        'delta_eth': 'ethereum profit/loss',
        'start_activity': 'start activity',
        'w_or_l':'winner or loser'
    },
    marginal = 'violin',
    histfunc = 'avg',
    title = 'When To Buy - At Mint Or Transfer?',
    width = 1000,
    height = 1000
)

# %%
# creating a line plot for 'when to sell'
cols = ['w_or_l', 'start_activity', 'delta_days', 'delta_eth']
mint_df = final_df[cols].query('start_activity == "mint"')

mint_df['delta_days_floor'] = mint_df['delta_days'].apply(math.floor)

cols = ['w_or_l', 'delta_days_floor', 'delta_eth']
mint_df = mint_df[cols].groupby(cols[:-1]).mean().reset_index()
mint_df = mint_df.sort_values(by = 'delta_days_floor')

scat = px.scatter(
    mint_df,
    x = 'delta_days_floor',
    y = 'delta_eth',
    color = 'w_or_l',
    hover_data = mint_df.columns,
    category_orders = {'w_or_l': ['winner', 'loser']},
    labels = {
        'delta_days_floor': 'days held',
        'delta_eth': 'ethereum profit/loss',
        'w_or_l': 'winner or loser'
    },
    opacity = .2,
    trendline = 'rolling',
    trendline_options = {'window': 2},
    title = 'When To Sell - Days Since Mint Purchase',
    width = 1000,
    height = 1000
)

# %%
# showing visuals based on output_type parameter
if args.output_type == 'streamlit':
    st.plotly_chart(hist)
    st.plotly_chart(scat)
else:
    hist.show()
    scat.show()

