import pandas as pd
import string
import re

#######################################
## path to coded guidance document snippets
path_to_gudiance = '../rsa/Coded Segments_Economic Data.xlsx'

#######################################
## path to output
out_csv = '../rsa/test_corpus.csv'

#######################################
## load guidance document snippets to (list of) dataframes
rsa = pd.read_excel(path_to_guidance,
                    sheet_name=[0,1,2])

#####################################
## Organization (single file):
## - each row contains {snippet, tag, source} (comma separated?)
## - for RSA tag: add to column {snippet, tag, source, RSA}

## select columns
columns_keep = ['snippet', 'tag_0', 'tag_1', 'tag_2', 'source']

df_corpus = pd.DataFrame(columns=columns_keep)

## iterate through dataframes and process as needed
for key, value in rsa.items():
    df = rsa[key]
    ## convert to string and remove hex codes and line breaks
    df['snippet'] = df['Segment'].apply(str).replace(r'[^\x00-\x7f]',r'', regex=True).apply(lambda x: x.replace('\n',''))
    ## split Code into multiple tags
    df[['tag_0', 'tag_1', 'tag_2']] = df['Code'].str.split('\\', expand=True)
    df.rename(columns={'Document name':'source'}, inplace=True)
    df_corpus = pd.concat([df_corpus, df[columns_keep]])

## Add unique IDs
df_corpus['unique_id'] = df_corpus.snippet.map(hash)

## write to csv
df_corpus.to_csv(out_csv, index=False)
