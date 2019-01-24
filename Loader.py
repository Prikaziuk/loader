import hashlib
import io
import os
import re
import shutil
import zipfile

from cli_parser import get_parser
from get_request import get_request
from LoaderDB import LoaderDB
from url_config import get_urls_and_query

# from logger_pkg import configure_logger
# logger = configure_logger(__name__,  # is duplicated by snappy but can't do anything
#                           handlers_levels={'print': 'DEBUG'},
#                           file_path='download_S3.log')

# if you don't have logger_pkg - use standard logging
import logging
# # if you want to control logs uncomment all lines
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)  # default logging.WARNING
logger = logging.getLogger()

RUN_FROM_CLI = True

# if RUN_FROM_CLI:

parser = get_parser()
args = parser.parse_args()
# args = parser.parse_args(['--help'])  # ['--query']
if args.s[0] in ('Sentinel-1', 'Sentinel-2', 'Sentinel-3') and args.a == ('s3guest', 's3guest'):
    parser.error("Sentinel-1,2,3 requires -a [credentials for sci.hub]")
print(args)


REGEX = r"(?<=<str name=\"{}\">).*(?=</str>)"
RE_DATE = r"(?<=<date name=\"beginposition\">).*(?=</date>)"
RE_NO_RESULTS = r"(?<=<subtitle>)(Displaying 0 results\.)"
RE_N_IMAGES = r'(?<=<opensearch:totalResults>)\d+'
RE_S2_CLOUDS = r'(?<=\"cloudcoverpercentage\">)\d+\.\d+'

MAX_REQUEST_N_IMAGES = 100
MAX_CLOUD_COVER = 90

# RE_OLCI_DATE = r"S3A_OL_1_EFR____\d{8}T\d{6}"
# RE_SLSTR_DATE = r"S3A_SL_1_RBT____\d{8}T\d{6}"
TYPICAL_CROPPED_LENGTH = 31
SLSTR_PATTERN = '_SL_'


B2MB = 1e-6

TMP_BYTES_PATH = './loaded'


class Loader:
    def __init__(self,
                 platform_name='Sentinel-3',
                 load_path='./',
                 cropped_path='./cropped',
                 auth=('username', 'password'),
                 product_type_or_level=None,
                 loader_db=LoaderDB(':memory:')):    # or 'productlevel:L1'
        self.url_dict, self.query_template = get_urls_and_query(platform_name)
        self.load_path = load_path
        self.cropped_path = cropped_path  # to check if already loaded
        self.db = loader_db

        if platform_name in ('Sentinel-1', 'Sentinel-2'):
            self.url_dict['auth'] = auth
        if product_type_or_level is None:
            product_type_or_level= self.url_dict['producttype']
            logger.warning('\n`product_type_or_level` was not specified. '
                           'Default type will be used for your platform: \n{}\n'.format(product_type_or_level))
        self.producttype = 'producttype:{}'.format(product_type_or_level)

    def download(self,
                 polygon='Nederland 2deg',  # or wkt
                 period=("2018-04-01", "2018-04-01")):
        results = self.query_copernicus(polygon, period)
        names = results['names']
        uuids = results['uuids']
        n_images = results['n_images']
        logger.info('Found {} images'.format(n_images))
        for i in range(n_images):  # for i, j in [(x, x + 1) for x in range(0, n_images, 2)]  # last one is step
            if i in results['i_clouded']:
                logger.warning('Too clouded - {}. Skipping'.format(names[i]))
                continue
            if '_T29TQE_' in names[i]:  # temporal measure for S2 as I don't know TQE TTK difference
                logger.info(f'SKIPPED TQE: {names[i]}')
                continue
            self.load_if_not_yet(uuids[i], names[i])

    def load_if_not_yet(self,
                        uuid,
                        name,
                        tmp_bytes_path=TMP_BYTES_PATH):
        file_in_cropped = self.is_file_in(self.cropped_path, name, cropped=True)
        file_in_loaded = self.is_file_in(self.load_path, name)
        if file_in_cropped or file_in_loaded:
            return
        loaded = self.download_timeout(uuid, tmp_bytes_path)
        if loaded is None:
            logger.critical('Was not able to download or check sums for image {} uuid {}'.format(name, uuid))
            return
        if self.url_dict['platformname'] == 'Sentinel-5':
            shutil.copyfile(tmp_bytes_path, self.load_path + name + '.nc')
        else:
            self.unzip_and_save_timeout(loaded, self.load_path)

    @staticmethod
    def is_file_in(path_to_folder, full_name, cropped=False):  # match => looking ONLY from the begging
        name_to_match = full_name
        len_match = 1
        if cropped:
            name_to_match = full_name[: TYPICAL_CROPPED_LENGTH]  # we usually cut like this
            if SLSTR_PATTERN in full_name:
                len_match = 2  # because we have to match SLSTR_1000, SLSTR_500
        if os.path.exists(path_to_folder):
            match = [x for x in os.listdir(path_to_folder) if re.match(name_to_match, x)]
            if len(match) == len_match:
                if cropped:  # making critical logs was not the best solution but now we can see in logs
                    logger.error('Cropped version of {} is already in {}'.format(full_name, path_to_folder))
                else:
                    logger.critical('Product {} was already downloaded to {}'.format(full_name, path_to_folder))
                return True
        return False

    def download_timeout(self,
                         uuid,
                         tmp_bytes_path):
        url_download = self.url_dict['url_download'].format(uuid)
        auth = self.url_dict['auth']

        # start_f = time.time()
        logger.info('Started downloading {}'.format(uuid))
        loaded, tried = get_request(url_download, auth, tmp_bytes_path)

        if loaded is None:
            logger.error('Was not able to download product {} retried {} times. Final size {} MB'.
                         format(uuid, tried, os.path.getsize(tmp_bytes_path) * B2MB))
            return

        if self.md5_ok(loaded, uuid):
            logger.info('{} successfully downloaded. MD5 sums were equal'.format(uuid))
            return loaded

    def md5_ok(self, loaded_content, uuid):
        url_md5 = self.url_dict['url_md5'].format(uuid)
        logger.debug('Started MD5 for {}'.format(uuid))

        md5_content, tried = get_request(url_md5, self.url_dict['auth'])

        if md5_content is None:
            logger.fatal('MD5 sums were not downloaded after {} attempts'.format(tried))
            return False

        loaded_md5 = hashlib.md5(loaded_content).hexdigest()
        expected_md5 = md5_content.decode('utf-8').lower()

        if loaded_md5 != expected_md5:
            logger.fatal('MD5 sums were not equal for {}'.format(uuid))
            return False
        return True

    @staticmethod
    def unzip_and_save_timeout(download_request_content, unzip_path):
        z = zipfile.ZipFile(io.BytesIO(download_request_content))
        z.extractall(unzip_path)
        name = z.namelist()[0][:-1]  # name of the folder + remove slash at the end
        logger.info(f'SUCCESSFULLY UNZIPPED AND SAVED \n {name} in {unzip_path}')

    def query_copernicus(self,
                         polygon='Nederland 2deg',  # or wkt
                         period=("2018-04-01", "2018-04-01")):
        results = {'uuids': [],
                   'names': [],
                   'dates': [],
                   'sizes': [],
                   'n_images': 0,
                   'clouds': [],
                   'i_clouded': []}

        url_search = self.url_dict['url_search']

        date_start, date_end = self.__parse_period(period)
        if self.db:
            wkt = self.__parse_polygon(polygon)
        else:
            logger.warning('Database was not selected so use wkt instead of a polygon name')
            wkt = polygon

        query = self.query_template.format(polygon=wkt,
                                           date_start=date_start,
                                           date_end=date_end,
                                           level_or_type=self.producttype,
                                           start='{start}')  # to keep {start} in formatted string
        start = 0
        search = url_search + query.format(start=start)
        # content, tried = self.get_request(search, 'query')
        content, tried = get_request(search, self.url_dict['auth'])

        if content is None:
            logger.error('Failed to get query {} after {} attempts'.format(query, tried))
            return results
        else:
            # request_text = r.text
            request_text = content.decode('utf-8')

        if re.findall(RE_NO_RESULTS, request_text):
            logger.warning('Query returned no results.\n{}'.format(query))
        else:
            n_images = int(re.search(RE_N_IMAGES, request_text).group())
            logger.debug('Found {} images'.format(n_images))
            while n_images - start > 0:
                start += MAX_REQUEST_N_IMAGES
                search = url_search + query.format(start=start)
                content, _ = get_request(search, self.url_dict['auth'])
                request_text += content.decode('utf-8')
            results = self.__parse_request_response(request_text)
            results['i_clouded'], results['clouds'] = self._find_clouds_s2(request_text)
            results['n_images'] = n_images
            if self.db:
                pol_id = self.db.get_pol_id(wkt)
                self.db.insert_query(self.url_dict, results, pol_id, self.producttype)
        return results

    @staticmethod
    def __parse_period(period):
        period_len = len(period)
        if isinstance(period, tuple):
            if period_len == 2:
                date_start, date_end = period
            elif period_len == 1:
                date_start = date_end = period[0]
            else:
                logger.error('Period has incorrect length. Expected 1 or 2, got {}'.format(period_len))
                return
        elif isinstance(period, str):
            date_start = date_end = period
        else:
            raise Exception('`period` has incorrect type. Expected "tuple" or "string", got {}'.format(type(period)))
        return date_start, date_end

    def __parse_polygon(self, polygon):
        if '(' in polygon:
            wkt = polygon
            self.db.insert_polygon(polygon)
        else:
            wkt = self.db.get_wkt_from_name(polygon)
            if wkt is None:
                raise Exception('Polygon with name `{}` was not found in the database. Provide correct name or just wkt'
                                .format(polygon))
        return wkt

    @staticmethod
    def __parse_request_response(request_text):
        results = {
            'dates': re.findall(RE_DATE, request_text),
            'uuids': re.findall(REGEX.format('uuid'), request_text),
            'names': re.findall(REGEX.format('identifier'), request_text),
            'sizes': re.findall(REGEX.format('size'), request_text)
        }
        return results

    def _find_clouds_s2(self, r_text):
        clouds = []
        i_clouded = []
        if self.url_dict['platformname'] == 'Sentinel-2':
            clouds = re.findall(RE_S2_CLOUDS, r_text)
            i_clouded = [i for i, x in enumerate(clouds) if float(x) > MAX_CLOUD_COVER]  # didn't want numpy
            logger.info('{} overcast images (> {}%) were found and will not be downloaded'
                        .format(len(i_clouded), MAX_CLOUD_COVER))
        return i_clouded, clouds


if __name__ == '__main__':
    """ for Sentinel-1 and 2 provide your credential in auth=('user', 'pwd')"""
    load_path_dir = r'./'
    # os.chdir(load_path)
    if args.database:
        db = LoaderDB('loader.db')
    else:
        db = LoaderDB(':memory:')

    loader = Loader(platform_name='Sentinel-3',
                    load_path=load_path_dir,
                    auth=args.a[0],
                    loader_db=db,
                    product_type_or_level=None)

    # print(os.getcwd())
    # if args.query:
    #     # results = loader.query_copernicus(polygon='Majadas EC', period=('2017-04-01', '2018-08-30'))
    #     print(loader.query_copernicus())
    # else:
    #     # loader.download(polygon='Majadas EC', period=('2017-04-01', '2018-04-01'))
    #     loader.download(polygon='Majadas EC', period=('2017-04-01', '2018-04-01'))
