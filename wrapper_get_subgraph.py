import sys, os, time
from datetime import datetime

from get_pajek_for_cluster import write_subgraph_to_pajek

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
logger = logging.getLogger(__name__)

def get_cluster_name(csv_fname, rownum, sep=','):
    with open(csv_fname, 'r') as f:
        i = 0
        for line in f:
            if i == rownum:
                line = line.strip().split(sep)
                cluster_name = line[0]
                break
            i += 1
    return cluster_name

def get_output_fname(fname_base, rownum, cluster_name):
    fname = "{}_{}_cluster_{}.net".format(fname_base, rownum, cluster_name)
    return fname

def main(args):
    cluster_name = get_cluster_name(args.csv_fname, args.rownum)
    logger.debug('Identified cluster name to extract: {}'.format(cluster_name))
    output_fname = get_output_fname(args.output_fname_base, args.rownum, cluster_name)
    linenums_fname = args.linenums_fname
    tree_fname = args.tree_fname
    input_pajek = args.input_pajek
    write_subgraph_to_pajek(linenums_fname, 
                            cluster_name, 
                            tree_fname, 
                            input_pajek,
                            output_fname)

if __name__ == "__main__":
    total_start = time.time()
    logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="wrapper to get one cluster")
    parser.add_argument("csv_fname", help="The filename for the csv file containing clusters and counts of nodes")
    parser.add_argument("rownum", type=int, help="row number in the csv file (assumes the csv file has header, so the first data row is 1)")
    parser.add_argument("linenums_fname", help="The filename for the CSV file containing the start lines in the tree file for each cluster")
    parser.add_argument("tree_fname", help="The .tree clustering file for the total network")
    parser.add_argument("input_pajek", help="The filename for the input pajek (.net) file containing the total network from which to get the subgraph")
    parser.add_argument("output_fname_base", help="The base for the filename for the output pajek file. If the base is e.g. 'network' and rownum is 5 then the output filename will be 'network_5_cluster_CLUSTERNAME.net'")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = time.time()
    logger.info('all finished. total time: {:.2f} seconds'.format(total_end-total_start))
