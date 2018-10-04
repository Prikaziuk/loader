import sqlite3

import logging
# # if you want to control logs uncomment all lines
# import sys
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)  # default logging.WARNING
logger = logging.getLogger()


class LoaderDB:
    """
    Has 2 tables to keep track of done things (query) and to store wkt of polygons, accessible by name (polygons)
    """
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.c = self.conn.cursor()
        # initialization of tables
        self._create_polygons_table()
        self._insert_known_polygons()
        self._create_query_table()

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

    def insert_query(self, url_dict, results, pol_id, product_type_or_level):
        length = results['n_images']
        platforms = [url_dict['platformname']] * length  # list of repeats
        levels = [product_type_or_level] * length
        pol_ids = [pol_id] * length
        if len(results['clouds']) == 0:  # `not results['clouds']` is not clear enough
            results['clouds'] = ['null'] * length
        with self.conn:
            self.c.executemany(
                """
                INSERT OR IGNORE INTO query
                (platformname, level_or_type, date, uuid, full_name, size, pol_id, clouds)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                zip(platforms, levels, results['dates'], results['uuids'], results['names'], results['sizes'],
                    pol_ids, results['clouds'])
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
    db = LoaderDB('loader.db')
    db._create_polygons_table()
    db._insert_known_polygons()
    db._create_query_table()

    print(db.get_pol_id("POLYGON ((3.0 54.0, 7.0 54.0, 7.0 50.0, 3.0 50.0, 3.0 54.0))"))
    print(db.get_wkt_from_name('Nederland 2deg'))
