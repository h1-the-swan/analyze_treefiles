import sys, os, time
from datetime import datetime

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
logger = logging.getLogger(__name__)

def process_tree_file(tree_fname, out_fname, sep=','):
    logger.debug("Opening output file: {}".format(out_fname))
    outf = open(out_fname, 'w')
    logger.debug("Opening input file: {}".format(tree_fname))
    with open(tree_fname, 'r') as f:
        line_num = 0
        current_cluster = ''
        num_clusters = 0
        for line in f:
            if line[0] != '#':
                line = line.strip().split(' ')
                cl = line[0].split(':')[0]

                if cl != current_cluster:
                    current_cluster = cl
                    num_clusters += 1
                    out_row = [cl, str(line_num)]
                    outf.write(sep.join(out_row))
                    outf.write("\n")
            line_num += 1

    outf.close()
    logger.info("Done writing {} clusters to file.".format(num_clusters))
    logger.debug("Length of input file was {} lines".format(line_num))

def main(args):
    infname = args.infname
    outfname = args.outfname
    if not outfname:
        outfname = "{}_treefile-cluster-linenumbers.csv".format(os.path.splitext(infname)[0])
    process_tree_file(infname, outfname)

if __name__ == "__main__":
    total_start = time.time()
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="Assuming that the clusters go in order for a tree file, get the (zero-based) line number at which each cluster starts and write it to a csv")
    parser.add_argument("infname", help="Input (.tree) filename")
    parser.add_argument("-o", "--outfname", default="", help="Output (.csv) filename. Two columns: cluster_id, line_number. If not specified, will use a default based on the input filename")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = time.time()
    logger.info('all finished. total time: {:.2f} seconds'.format(total_end-total_start))
