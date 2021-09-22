import pandas as pd
import string
import re

#######################################
## path to coded guidance document snippets
path_to_guidance = '../data/raw/ras/V3RASCoded Segments_all_details.xlsx'
## sample segments:
path_to_list = '../data/raw/ras/sample/Coded Segments_Economic Data.xlsx'

#######################################
## path to output
out_csv = '../data/processed/corpus_guidance.csv'

#######################################
## load guidance document snippets to (list of) dataframes
ras = pd.read_excel(path_to_guidance)
## sample segments:
ras_list = pd.read_excel(path_to_list, sheet_name=[0,1,2])

#####################################
## Organization (single file):
## - each row contains {Doc group, Doc name, Code, Segment}
## - Doc group: high-level RAS tag (group)
## - Doc name: Source document
## - Code: tag_0\tag_1\tag_2
## - Segment: text

def make_corpus(df, group=False, author=False):
    ## make a copy
    corpus = df.copy()
    ## set columns for new df
    columns_keep = ['unique_id', 'snippet', 'tag_0', 'tag_1', 'tag_2', 'source']
    if group:
        columns_keep.append('group')
    if author:
        columns_keep.append('author')
        
    ## convert text to string and remove hex codes and line breaks
    corpus['snippet'] = corpus['Segment'].apply(str).replace(r'[^\x00-\x7f]',r'', regex=True).apply(lambda x: x.replace('\n',''))
    ## split Code into multiple tags
    corpus[['tag_0', 'tag_1', 'tag_2']] = corpus['Code'].str.split('\\', expand=True)
    ## rename source and group
    corpus.rename(columns={'Document name':'source'}, inplace=True)
    if group:
        corpus.rename(columns={'Document group':'group'}, inplace=True)
    ## add annotator?  
    if author:
        corpus.rename(columns={'Created by':'author'}, inplace=True)

    ## add unique ID by hashing
    ## corpus['unique_id'] = corpus.snippet.map(hash)
    ## add unique sequential ID
    corpus['unique_id'] = corpus.index
    return corpus[columns_keep]


## if there are multiple sheets, ie, a list of df's
def corpus_from_list(dflist, group=False):
    corpus_list = []
    ## iterate through dataframes and process as needed
    for key, value in dflist.items():
        df = dflist[key].copy()
        df = make_corpus(df, group=group)
        corpus_list.append(df)

    return pd.concat(corpus_list)


## The sample corpus
df_corpus_sample = corpus_from_list(ras_list)

## The whole corpus in one file (no list)
df_corpus = make_corpus(ras, group=True)

## write to csv
df_corpus.to_csv(out_csv, index=False)

df_corpus_sample.to_csv('../data/processed/test_corpus.csv', index=False)
