import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required arguments')
    parser.add_argument('-s', metavar='platformname', type=str, nargs=1,
                        default='Sentinel-3',
                        help='Name of the Sentinel platform. Default: Sentinel-3',
                        choices=['Sentinel-1', 'Sentinel-2', 'Sentinel-3', 'Sentinel-3_pre', 'Sentinel-5'])

    parser.add_argument('-t', metavar='tmp_path', type=str, nargs=1,
                        default='./',
                        help='Path to the folder for tmp file. Default: ./')

    parser.add_argument('-o', metavar='load_path', type=str, nargs=1,
                        default='./',
                        help='Path to the folder for downloaded product. Default: ./')

    parser.add_argument('-c', metavar='cropped_path', type=str, nargs=1,
                        default='./cropped',
                        help='Path to the folder with cropped products (subsets) to check '
                             'if the file was already downloaded and cropped. Default: ./cropped')

    parser.add_argument('-d',  metavar='days', type=str, nargs=2,
                        default='2018-04-01 2018-04-01',
                        help='Start date, end date YYYY-mm-dd')

    parser.add_argument('-p',  metavar='polygon', type=str, nargs=1,
                        default="POLYGON ((3.0 54.0, 7.0 54.0, 7.0 50.0, 3.0 50.0, 3.0 54.0))",
                        help='polygon in wkt format or the name of polygon from the database if it was created')

    parser.add_argument('-a',  metavar='credentials', type=tuple, nargs=1,
                        default=('s3guest', 's3guest'),
                        help='auth for copernicus sci.hub (REQUIRED for Sentinel-1, 2)')

    parser.add_argument('--query', action='store_true',
                        help='Flag to do ONLY the query without downloading data')

    parser.add_argument('--database', action='store_true',
                        help='Flag to write and use database file (loader.db) instead of in-memory database')
    return parser
