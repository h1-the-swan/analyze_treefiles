
# coding: utf-8

# In[1]:

import sys, os, time
import cPickle as pickle
from glob import glob


# In[2]:

from sqlalchemy import create_engine, MetaData, select


# In[3]:

import pandas as pd
import numpy as np


total_start = time.clock()

# In[4]:

engine = create_engine('mysql+mysqldb://jason:password@localhost/wos')
metadata = MetaData(engine)
metadata.reflect()


# In[5]:

def parse_tree_one_line(line):
    line = line.strip().split(' ')
    cl = line[0]
    paper_id = line[2].strip('"')
    return paper_id, cl

with open('data/relaxmap_cluster_treefiles/wos_2_cluster_4654150.tree', 'r') as f:
    rows = []
    for line in f:
        if line[0] != "#":
            row = parse_tree_one_line(line)
            rows.append(row)
df = pd.DataFrame(rows, columns=['UID', 'cl'])
df['cl_bottom'] = df['cl'].apply(lambda x: ':'.join(x.split(':')[:-1]))

df['cl_top'] = df['cl'].apply(lambda x: x.split(':')[0])


# In[6]:

cl = '1:1:1'
subset = df[df.cl_bottom==cl]

t_uid = subset.iloc[0]['UID']


tbl = metadata.tables['pubInfo_minimal']
sq = tbl.select(tbl.c.UID==t_uid)
df_result = pd.read_sql(sq, engine)

uids = subset['UID'].tolist()
tbl = metadata.tables['pubInfo_minimal']
sq = tbl.select(tbl.c.UID.in_(uids))
df_result = pd.read_sql(sq, engine)


# In[7]:

for colname in df_result.columns.tolist():
    notnull = df_result[colname].notnull().sum()
    print("{}: {}".format(colname, notnull))


# In[8]:

df_result.heading.value_counts()


# In[9]:

for cl, cnt in df.cl_bottom.value_counts().iteritems():
    print(cl, cnt)


# In[ ]:




# In[10]:

def get_query_result(df_tree, cl, engine, metadata):
    subset = df_tree[df_tree['cl_bottom']==cl]

    uids = subset['UID'].tolist()
    tbl = metadata.tables['pubInfo_minimal']
    sq = tbl.select(tbl.c.UID.in_(uids))
    df_result = pd.read_sql(sq, engine)
    return df_result


# In[11]:

def interpret_query_result(df_result):
    d = dict()
    d['counts'] = dict()
    for colname in df_result.columns.tolist():
        notnull = df_result[colname].notnull().sum()
        d['counts'][colname] = notnull
    d['heading_value_counts'] = df_result['heading'].value_counts()
    d['pubyear_value_counts'] = df_result['pubyear'].value_counts()
    d['subheading_value_counts'] = df_result['subheading'].value_counts()
    d['subject_extended_value_counts'] = df_result['subject_extended'].value_counts()
    d['title_source_value_counts'] = df_result['title_source'].value_counts()
    return d


# In[12]:

def parse_tree_one_line(line):
    line = line.strip().split(' ')
    cl = line[0]
    paper_id = line[2].strip('"')
    return paper_id, cl

def parse_tree(fname):
    with open(fname, 'r') as f:
        rows = []
        for line in f:
            if line[0] != "#":
                row = parse_tree_one_line(line)
                rows.append(row)
                
    df = pd.DataFrame(rows, columns=['UID', 'cl'])
    
    df['cl_bottom'] = df['cl'].apply(lambda x: ':'.join(x.split(':')[:-1]))

    df['cl_top'] = df['cl'].apply(lambda x: x.split(':')[0])
    
    return df


# In[13]:

def get_results_for_multiple_clusters(df_tree, n=10, engine=engine, metadata=metadata):
    # get results for biggest n clusters from one tree file
    d = dict()
    i = 0
    for cl, cnt in df_tree['cl_bottom'].value_counts().iteritems():
        d[cl] = dict()
        d[cl]['total_count'] = cnt
        df_result = get_query_result(df_tree, cl, engine, metadata)
        d_result = interpret_query_result(df_result)
        for k, v in d_result.items():
            d[cl][k] = v
        i += 1
        if i == n:
            break
    return d


# In[14]:

def get_fname_from_number(number):
    g = glob('data/relaxmap_cluster_treefiles/wos_{}_cluster*.tree'.format(number))
    if len(g) == 1:
        return g[0]
    else:
        raise RuntimeError("number: {} -- file not found".format(number))


# In[ ]:


d = dict()
for i in range(1, 70):
    try:
        fname = get_fname_from_number(i)
        print("parsing tree for {}...".format(fname))
        start = time.clock()
        df_tree = parse_tree(fname)
        print("done. {:.1f} seconds. getting results...".format(time.clock()-start))
        fname_tail = os.path.split(fname)[1]
        start = time.clock()
        d[fname_tail] = get_results_for_multiple_clusters(df_tree, n=10)
        print("collected data for relaxmap cluster {}. {:.1f} seconds".format(i, time.clock()-start))
    except RuntimeError:
        print("relaxmap cluster {} tree file not found. skipping".format(i))


# In[ ]:

def print_results1(relaxmap_cluster_data, colname):
    fieldname = "{}_value_counts".format(colname)
    vals = []
    for cl, d in relaxmap_cluster_data.iteritems():
        x = d[fieldname]
        topitem = x.index[0]
        vals.append(topitem)
    print(" | ".join(vals))


# In[ ]:

def results2_get_topitem(s):
    # if top two values are within [10%] of each other, return both
    a, b = s.values[:2]
    err = float(a - b) / a
    if err < .1:
        return "{}/{}".format(s.index[0], s.index[1])
    else:
        return s.index[0]
    
    

def print_results2(relaxmap_cluster_data, colname):
    fieldname = "{}_value_counts".format(colname)
    vals = []
    for cl, d in relaxmap_cluster_data.iteritems():
        x = d[fieldname]
        topitem = results2_get_topitem(x)
        vals.append(topitem)
    print(" | ".join(vals))


# In[ ]:


for k, v in sorted(d.items()):
    print(k)
    print_results1(v, 'subject_extended')
    print("")


# In[ ]:

for k, v in sorted(d.items()):
    print(k)
    print_results1(v, 'title_source')
    print("")


# In[ ]:

for k, v in sorted(d.items()):
    print(k)
    print_results2(v, 'subject_extended')
    print("")

outfname = 'data/relaxmapclusters_vc_1-70.pickle'
print("writing to {}".format(outfname))
with open(outfname, 'wb') as outf:
    pickle.dump(d, outf)

total_end = time.clock()
print("all done. total time {:.1f} seconds".format(total_end-total_start))
