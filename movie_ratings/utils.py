import argparse


def parse_args(args):
    parser = argparse.ArgumentParser(description='PySpark application to calculate two new tables based of movies and '
                                                 'ratings.')

    # add the input argument to the parser
    parser.add_argument('-m', '--movies', type=str, required=True, dest='movies',
                        help='Path to input movies input file.')
    parser.add_argument('-r', '--ratings', type=str, required=True, dest='ratings',
                        help='Path to input ratings input file.')

    # add the output argument to the parser
    parser.add_argument('-o', '--output', type=str, required=True, dest='output',
                        help='Path to output folder.')

    args = parser.parse_args(args)
    return args