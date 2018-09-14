import hashlib
import io
import os
import re
import requests
import shutil
import sqlite3
import time
import zipfile

from url_config import get_urls_and_query

# from logger_pkg import configure_logger
# logger = configure_logger(__name__,  # is duplicated by snappy but can't do anything
#                           handlers_levels={'print': 'DEBUG'},
#                           file_path='download_S3.log')

# if you don't have logger_pkg - use standard logging
import logging
# # if you want to control logs uncomment all lines
# import sys
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)  # default logging.WARNING
logger = logging.getLogger()


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

TRY_RECONNECT = 3
REQUEST_TIMEOUT = 5
SLEEP_NOT_OK = 1800
DOWNLOAD_TIMEOUT = 900
SEC_2_MIN = 1 / 60
B2MB = 1e-6

TMP_BYTES_PATH = './loaded'


class Loader:
    def __init__(self,
                 platform_name='Sentinel-3',
                 load_path='./',
                 cropped_path='./cropped',
                 auth=('username', 'password')):
        self.conn = sqlite3.connect('loader.db')
        self.c = self.conn.cursor()
        self.url_dict, self.query_template = get_urls_and_query(platform_name)
        self.load_path = load_path
        self.cropped_path = cropped_path  # to check if already loaded
        if platform_name in ('Sentinel-1', 'Sentinel-2'):
            self.url_dict['auth'] = auth

    def download(self,
                 polygon='Nederland 2deg',  # or wkt
                 period=("2018-04-01", "2018-04-01"),
                 product_type_or_level=None):  # or 'productlevel:L1'
        uuids, names, i_clouded = self.query_copernicus(polygon, period, product_type_or_level)
        n_images = len(uuids)
        logger.info('Found {} images'.format(n_images))
        for i in range(n_images):  # for i, j in [(x, x + 1) for x in range(0, n_images, 2)]  # last one is step
            if i in i_clouded:
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

        start_f = time.time()
        logger.info('Started downloading {}'.format(uuid))

        loaded = None
        tried = 0
        while tried < TRY_RECONNECT:
            tried += 1
            logger.debug('Connecting... attempt # {}'.format(tried))
            timeout = False
            start = time.time()
            try:
                r = requests.get(url_download, auth=auth, stream=True, timeout=REQUEST_TIMEOUT)
                if not r.ok:
                    logger.warning('Was not able to download product {}. Status code {}'.format(uuid, r.status_code))
                    time.sleep(SLEEP_NOT_OK)
                    continue
                os.makedirs(os.path.dirname(tmp_bytes_path), exist_ok=True)
                with open(tmp_bytes_path, 'wb') as tmp:
                    for chunk in r.iter_content(chunk_size=1024):
                        # if chunk:  # filter out keep-alive new chunks
                        tmp.write(chunk)
                        if time.time() - start > DOWNLOAD_TIMEOUT:
                            r.close()
                            logger.warning('Custom timeout on download {}'.format(tried))
                            timeout = True
                            break
            except Exception as e:  # may be (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
                passed = time.time() - start
                logger.warning('Download product {}; exception: {}; {} seconds passed'.
                               format(uuid, e.__class__, passed))
                # time.sleep(SLEEP)  # time to maybe restore the connection
                continue  # this is needed because timeout==False in case of exceptions

            if not timeout:
                with open(tmp_bytes_path, 'rb') as tmp:
                    loaded = tmp.read()
                    break

        elapsed = round(time.time() - start_f, 2)
        logger.debug('Elapsed \t{}\t min\n'.format(elapsed * SEC_2_MIN))

        if loaded is None:
            logger.error('Was not able to download product {} within {} mins, retried {} times. Final size {} MB'.
                         format(uuid, elapsed * SEC_2_MIN, tried, os.path.getsize(tmp_bytes_path) * B2MB))
            return

        if self.md5_ok(loaded, uuid):
            logger.info('{} successfully downloaded. MD5 sums were equal'.format(uuid))
            return loaded

    def md5_ok(self, loaded_content, uuid):
        url_md5 = self.url_dict['url_md5'].format(uuid)
        logger.debug('Started MD5 for {}'.format(uuid))

        md5_request, tried = self.get_request(url_md5, 'md5')

        if md5_request is None:
            logger.fatal('MD5 sums were not downloaded after {} attempts'.format(tried))
            return False

        loaded_md5 = hashlib.md5(loaded_content).hexdigest()
        expected_md5 = md5_request.text.lower()

        if loaded_md5 != expected_md5:
            logger.fatal('MD5 sums were not equal for {}'.format(uuid))
            return False
        return True

    def get_request(self, url, purpose='md5'):
        r = None
        tried = 0
        while r is None and tried < TRY_RECONNECT:
            tried += 1
            logger.debug('Connecting... attempt # {}'.format(tried))
            try:
                r = requests.get(url, auth=self.url_dict['auth'], timeout=REQUEST_TIMEOUT)
                if not r.ok:
                    logger.error('Status code {}. {} {}'.format(r.status_code, purpose.upper(), url))
                    time.sleep(SLEEP_NOT_OK)
                    r = None
            except Exception as e:
                logger.warning(
                    'Exception: {}: {}'.format(e.__class__, purpose.upper()))
                # time.sleep(SLEEP)  # maybe time to restore connection
                r = None
        return r, tried

    @staticmethod
    def unzip_and_save_timeout(download_request_content, unzip_path):
        z = zipfile.ZipFile(io.BytesIO(download_request_content))
        z.extractall(unzip_path)
        name = z.namelist()[0][:-1]  # name of the folder + remove slash at the end
        logger.info(f'SUCCESSFULLY UNZIPPED AND SAVED \n {name} in {unzip_path}')

    def query_copernicus(self,
                         polygon='Nederland 2deg',  # or wkt
                         period=("2018-04-01", "2018-04-01"),
                         product_type_or_level=None):  # or 'productlevel:L1'
        if product_type_or_level is None:
            product_type_or_level = 'producttype:{}'.format(self.url_dict['producttype'])
            logger.warning('\n`product_type_or_level` was not specified. '
                           'Default type will be used for your platform: \n{}\n'.format(product_type_or_level))
            # make levels list, suggest choice
        uuids = []
        names = []
        i_clouded = []

        url_search = self.url_dict['url_search']

        date_start, date_end = self.__parse_period(period)
        wkt = self.__parse_polygon(polygon)

        query = self.query_template.format(polygon=wkt,
                                           date_start=date_start,
                                           date_end=date_end,
                                           level_or_type=product_type_or_level,
                                           start='{start}')  # to keep {start} in formatted string
        start = 0
        search = url_search + query.format(start=start)
        r, tried = self.get_request(search, 'query')

        if r is None:
            logger.error('Failed to get query {} after {} attempts'.format(query, tried))
        elif re.findall(RE_NO_RESULTS, r.text):
            logger.warning('Query returned no results.\n{}'.format(query))
        else:
            request_text = r.text
            n_images = int(re.search(RE_N_IMAGES, request_text).group())
            logger.debug('Found {} images'.format(n_images))
            while n_images - start > 0:
                start += MAX_REQUEST_N_IMAGES
                search = url_search + query.format(start=start)
                r, _ = self.get_request(search, 'query')
                request_text += r.text
            pol_id = self.get_pol_id(wkt)
            dates, uuids, names, sizes = self.__parse_request_response(request_text)
            i_clouded, clouds = self._find_clouds_s2(request_text)
            self._insert_query(dates, uuids, names, sizes, pol_id, product_type_or_level, clouds)
        return uuids, names, i_clouded

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
            self.insert_polygon(polygon)
        else:
            wkt = self.get_wkt_from_name(polygon)
            if wkt is None:
                raise Exception('Polygon with name `{}` was not found in the database. Provide correct name or just wkt'
                                .format(polygon))
        return wkt

    @staticmethod
    def __parse_request_response(request_text):
        dates = re.findall(RE_DATE, request_text)
        uuids = re.findall(REGEX.format('uuid'), request_text)
        names = re.findall(REGEX.format('identifier'), request_text)
        sizes = re.findall(REGEX.format('size'), request_text)
        return dates, uuids, names, sizes

    def _find_clouds_s2(self, r_text):
        clouds = []
        i_clouded = []
        if self.url_dict['platformname'] == 'Sentinel-2':
            clouds = re.findall(RE_S2_CLOUDS, r_text)
            i_clouded = [i for i, x in enumerate(clouds) if float(x) > MAX_CLOUD_COVER]  # didn't want numpy
            logger.info('{} overcast images (> {}%) were found and will not be downloaded'
                        .format(len(i_clouded), MAX_CLOUD_COVER))
        return i_clouded, clouds

    def _insert_query(self, dates, uuids, names, sizes, pol_id, product_type_or_level, clouds):
        platforms = [self.url_dict['platformname']] * len(dates)  # list of repeats
        levels = [product_type_or_level] * len(dates)
        pol_ids = [pol_id] * len(dates)
        if len(clouds) == 0:
            clouds = ['null'] * len(dates)
        with self.conn:
            self.c.executemany(
                """
                INSERT OR IGNORE INTO query
                (platformname, level_or_type, date, uuid, full_name, size, pol_id, clouds)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                zip(platforms, levels, dates, uuids, names, sizes, pol_ids, clouds)
            )

    def _create_query_table(self):
        with self.conn:
            self.c.execute(
                """
                CREATE TABLE IF NOT EXISTS query 
                (
                id INTEGER PRIMARY KEY,
                platformname TEXT,
                level_or_type TEXT,
                date TEXT,
                uuid TEXT,
                full_name TEXT,
                size TEXT, 
                clouds REAL, 
                pol_id INT REFERENCES polygons (pol_id),
                CONSTRAINT unq UNIQUE (pol_id, uuid)
                )
                """
            )

    def _create_polygons_table(self):
        with self.conn:
            self.c.execute(
                """
                CREATE TABLE IF NOT EXISTS polygons 
                (
                pol_id INTEGER PRIMARY KEY,
                polygon_name TEXT,
                wkt TEXT UNIQUE,
                CONSTRAINT unq UNIQUE (polygon_name, wkt)
                )
                """
            )

    def insert_polygon(self, wkt, name=""):
        with self.conn:
            self.c.execute(
                """
                INSERT OR IGNORE INTO polygons 
                (wkt, polygon_name) 
                VALUES (?, ?)
                """,
                (wkt, name)
            )

    def get_pol_id(self, wkt):
        self.c.execute(
            """
            SELECT pol_id
            FROM polygons
            WHERE wkt = ?
            """,
            (wkt, )
        )
        res = self.c.fetchone()
        if res is None:
            logger.debug('Polygon {} was not found in polygons table'.format(wkt))
        else:
            res = res[0]  # removing tuple
        return res

    def get_wkt_from_name(self, polygon_name):
        self.c.execute(
            """
            SELECT wkt
            FROM polygons
            WHERE polygon_name = ?
            """,
            (polygon_name, )
        )
        res = self.c.fetchone()
        if res is None:
            logger.debug('Polygon {} was not found in polygons table'.format(polygon_name))
        else:
            res = res[0]  # removing tuple
        return res

    def _insert_known_polygons(self):
        self.insert_polygon('POLYGON ((-5.780806639544274 39.94849220488383, '
                            '-5.765431071920511 39.94680338547788, '
                            '-5.767758852013342 39.934209156869166,'
                            ' -5.783131547812546 39.93589853110727, '
                            '-5.780806639544274 39.94849220488383))', 'Majadas EC')
        self.insert_polygon("POLYGON ((3.0 54.0, 7.0 54.0, 7.0 50.0, 3.0 50.0, 3.0 54.0))", 'Nederland 2deg')


if __name__ == '__main__':
    """ for Sentinel-1 and 2 provide your credential in auth=('user', 'pwd')"""
    load_path_dir = r'./'
    # os.chdir(load_path)
    loader = Loader(platform_name='Sentinel-2',
                    load_path=load_path_dir,
                    auth=('user', 'pwd'))
    # print(os.getcwd())

    loader._create_polygons_table()
    loader._insert_known_polygons()
    loader._create_query_table()

    print(loader.get_pol_id("POLYGON ((3.0 54.0, 7.0 54.0, 7.0 50.0, 3.0 50.0, 3.0 54.0))"))
    print(loader.get_wkt_from_name('Nederland 2deg'))
    print(loader.query_copernicus())

    # names, uuids, _ = loader.query_copernicus(polygon='Majadas EC', period=('2017-04-01', '2018-08-30'))
    loader.download(polygon='Majadas EC', period=('2017-04-01', '2018-04-01'), product_type_or_level='S2MSI2Ap')
