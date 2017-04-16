import sys, os, time
from datetime import datetime

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_and_replace(input_filename, output_filename, 
                        replace_map={'\n': 'n', 
                                    '\t': 't', 
                                    '\r': 'r'}):
    """Read the csv file one character at a time
    Replace any of the three special characters within fields
    Output to file

    :input_filename: TODO
    :output_filename: TODO

    """
    logger.debug('opening output file {}'.format(output_filename))
    outf = open(output_filename, 'wb')

    with open(input_filename, 'rb') as f:
        offset = 0  # character offset
        current_line_offset = 0
        line_num = 0
        in_quote = False
        fields_this_row = 0
        special = set(replace_map.keys())
        last_char = ''

        # begin loop. read one character at a time
        while True:
            char = f.read(1)
            offset += 1
            if not char:
                # End of file
                break

            if char == '"':
                # if the last character was a backslash, this is an escaped quotation mark and should be ignored
                if last_char != '\\':
                    in_quote = not in_quote  # toggle
                    if in_quote:
                        fields_this_row += 1

            if char in special:
                if in_quote:
                    # we have encountered a special character within a data field.
                    # replace it
                    char = replace_map[char]
                else:
                    if char == '\n':
                        # new line
                        line_num += 1
                        fields_this_row = 0
                        current_line_offset = offset
            # write this character to file
            outf.write(char)
            last_char = char  # store last character for next time thru the loop


    outf.close()

def main(args):
    parse_and_replace(args.input_filename, args.output_filename)

if __name__ == "__main__":
    total_start = time.clock()
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="The pubInfo.csv file breaks standard parsers because it contains special characters (\"\\n\", \"\\t\", \"\\r\"). This script will go through and replace the special characters within fields (\"\\n\" -> \"n\", \"\\t\" -> \"t\", \"\\r\" -> \"r\")")
    parser.add_argument("input_filename", help="input filename (e.g. pubInfo.csv)")
    parser.add_argument("output_filename", help="output (csv) filename")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = time.clock()
    logger.info('all finished. total time: {:.2f} seconds'.format(total_end-total_start))
