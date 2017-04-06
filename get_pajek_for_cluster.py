import sys, os, time
from datetime import datetime

import pandas as pd
import numpy as np

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S")
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

def extract_subgraph_and_get_lines_for_pajek(fname_pjk, paper_ids):
    """Write a new pajek (.net) file for just a given set of paper ids

    :fname_pjk: input pajek (.net) filename
    :paper_ids: set of paper ids to identify in the input network
    :returns: lines = {"vertices": lines to write to vertices section of output file,
                        "edges": lines to write to edges section of output file}

    """
    lines = {
                'vertices': [],
                'edges': []
            }
    paper_indices = []

    with open(fname_pjk, 'r') as f:
        i = 0
        mode = ''
        new_idx = 0
        old_new_idx_dict = {}
        for line in f:
            if line[0] == '*':
                if line[1].lower() == 'v':  # this should happen on the very first line
                    mode = 'v'
                elif line[1].lower() in ['a', 'e']:
                    paper_indices = set(paper_indices)
                    mode = 'e'
                continue

            if mode == 'v':
                items = line.strip().split(' ')
                paper_index = items[0]
                paper_id = items[1].strip('"')
                if paper_id in paper_ids:
                    new_idx += 1  # we want the new idx to start at 1
                    paper_indices.append(paper_index)
                    old_new_idx_dict[paper_index] = new_idx
                    output_line = ' '.join([new_idx] + items[1:])
                    output_line = output_line + '\n'
                    lines['vertices'].append(output_line)
            elif mode == 'e':
                # edges
                items = line.strip().split(' ')
                citing_index = items[0]
                # we only care if both citing and cited are in this line
                if citing_index in paper_indices:
                    cited_index = items[1]
                    if cited_index in paper_indices:
                        citing_index_new = old_new_idx_dict[citing_index]
                        cited_index_new = old_new_idx_dict[cited_index]
                        output_line = ' '.join([str(citing_index_new), str(cited_index_new)])
                        lines['edges'].append(output_line)
            i += 1
    return lines

def write_pajek(lines, outfname):
    """Write output pajek (.net) file

    :lines: dictionary: {"vertices": lines to write to vertices section of output file (list),
                        "edges": lines to write to edges section of output file (list)}
    :outfname: output pajek (.net) file name

    The lines in <lines> should contain newline characters at the end of each line

    """
    with open(outfname, 'w') as outf:
        num_vertices = len(lines['vertices'])
        logger.debug('writing {} vertices...'.format(num_vertices))
        outf.write('*vertices {}\n'.format(num_vertices))
        i = 0
        for line in lines['vertices']:
            outf.write(line)

        num_edges = len(lines['edges'])
        logger.debug('writing {} arcs...'.format(num_edges))
        outf.write('*arcs {}\n'.format(num_edges))
        for line in lines['edges']:
            outf.write(line)
    

    

def get_paper_ids_for_cluster(fname_tree, cl_name, cl_linenumber):
    """Get the paper ids for a single cluster from the tree file

    :fname_tree: filename of the tree file containing the clustering
    :cl_name: name of the cluster
    :cl_linenumber: starting line number for the cluster in the tree file
    :returns: paper ids as a set

    """
    paper_ids = []
    active = False
    i = 0

    with open(fname_tree, 'r') as f:
        for line in f:
            if i == cl_linenumber:
                active = True
            if active is True:
                line = line.strip().split(' ')
                this_cl = line[0].split(':')[0]
                this_paper_id = line[2].strip('"')
                if this_cl == cl_name:
                    paper_ids.append(this_paper_id)
                else:
                    active = False
            i += 1
    
    paper_ids = set(paper_ids)
    return paper_ids

def get_linenumber_for_cluster(linenumbers, cluster_name):
    """Get the starting line number in the tree file for a single cluster
    """
    return linenumbers[cluster_name]

def get_linenumbers(fname_linenumbers):
    """return map of cluster_id to linenumber in the tree file as Pandas series

    """
    linenumbers = pd.read_csv(fname_linenumbers, header=None, index_col=0, squeeze=True)
    linenumbers.index = linenumbers.index.astype('str')
    return linenumbers

def write_subgraph_to_pajek(linenums_fname, 
                            cluster_name, 
                            tree_fname, 
                            input_pajek, 
                            output_fname):
    start = time.clock()
    logger.debug('getting map of cluster_id to linenumber in the tree file...')
    linenumbers = get_linenumbers(linenums_fname)
    logger.debug('done. took {:.1f} s'.format(time.clock()-start))

    start = time.clock()
    logger.debug('getting line number for cluster {}...'.format(cluster_name))
    cl_linenumber = get_linenumber_for_cluster(linenumbers, cluster_name)
    logger.debug('done. took {:.1f} s'.format(time.clock()-start))
    logger.debug('linenumber: {}'.format(cl_linenumber))

    start = time.clock()
    logger.debug('getting paper ids from treefile {}...'.format(tree_fname))
    paper_ids = get_paper_ids_for_cluster(tree_fname, cluster_name, cl_linenumber)
    logger.debug('done. took {:.1f} s'.format(time.clock()-start))
    logger.debug('found {} paper ids.'.format(len(paper_ids)))

    start = time.clock()
    logger.debug('extracting subgraph from input pajek file {}...'.format(input_pajek))
    lines = extract_subgraph_and_get_lines_for_pajek(input_pajek, paper_ids)
    logger.debug('done. took {:.1f} s'.format(time.clock()-start))

    start = time.clock()
    logger.debug('writing to file {}...'.format(output_fname))
    write_pajek(lines, output_fname)
    logger.debug('done. took {:.1f} s'.format(time.clock()-start))


def main(args):
    write_subgraph_to_pajek(args.linenums_fname, 
                            args.cluster_name, 
                            args.tree_fname, 
                            args.input_pajek,
                            args.output_fname)

    

if __name__ == "__main__":
    total_start = time.time()
    logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="Given a pajek (.net) file of the total network, and a .tree clustering file, and the name of a single cluster, output a pajek (.net) file for just the cluster (subgraph)")
    parser.add_argument("input_pajek", help="The filename for the input pajek (.net) file containing the total network from which to get the subgraph")
    parser.add_argument("tree_fname", help="The .tree clustering file for the total network")
    parser.add_argument("cluster_name", help="The name of the cluster you want to extract (probably an integer)")
    parser.add_argument("linenums_fname", help="The filename for the CSV file containing the start lines in the tree file for each cluster")
    parser.add_argument("output_fname", help="The name of the output (pajek - .net) file")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = time.time()
    logger.info('all finished. total time: {:.2f} seconds'.format(total_end-total_start))
