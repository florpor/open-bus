import psycopg2
from psycopg2.extensions import AsIs
import csv
from collections import namedtuple
import os
import zipfile
import datetime
import logging

# todo:
# stops record matcher should test point as well (with tolerance?)


class LoaderConfig:
    def __init__(self,
                 csv_record_supplier=None,
                 record_matcher=None,
                 record_to_csv=None,
                 gtfs_base_file_name=None,
                 db_table=None,
                 db_id_field=None,
                 db_fields=None):
        self.csv_record_supplier = csv_record_supplier
        self.record_matcher = record_matcher
        self.record_to_csv = record_to_csv
        self.gtfs_base_file_name = gtfs_base_file_name
        self.db_table = db_table
        self.db_id_field = db_id_field
        self.db_fields = db_fields


class Loader:
    def __init__(self, db_connection, gtfs_folder, gtfs_date, tmp_folder, loader_config: LoaderConfig):
        self.db_connection = db_connection
        self.gtfs_folder = gtfs_folder
        self.gtfs_date = gtfs_date
        self.tmp_folder = tmp_folder
        self.db_ids_in_use = set()
        self.db_cursor = db_connection.cursor()

        self.records_file_name = os.path.join(self.tmp_folder, '%s.txt' % loader_config.gtfs_base_file_name)
        self.tmp_ids_filename = os.path.join(self.tmp_folder, '%s_ids_in_use.txt' % loader_config.gtfs_base_file_name)

        self.outf = open(self.records_file_name, 'w', encoding='utf8')
        self.writer = csv.DictWriter(self.outf, loader_config.db_fields, lineterminator='\n')

        self.csv_record_supplier = loader_config.csv_record_supplier
        self.record_matcher = loader_config.record_matcher
        self.record_to_csv = loader_config.record_to_csv

        self.db_table = loader_config.db_table
        self.db_id_field = loader_config.db_id_field

        self.db_cursor.execute('SELECT max(%s) FROM %s' % (self.db_id_field, self.db_table))
        r = self.db_cursor.fetchone()
        self.next_id = r[0] + 1 if r[0] else 1

    def load(self):
        """

:param csv_record_supplier: a generator that returns record objects  
:param record_matcher: a function that receives a record and returns the current db id for it, or None if no match
:param in_use: a function used to signal that an existing db record is still in use
:param add_new: a function that receives a new record that needs to persisted to db
:param finalize: a function called at the end and can be used to finalise any db 
:return: 
"""
        csv_id_to_db_id = {}
        for record in self.csv_record_supplier():
            db_id = self.record_matcher(record)
            if not db_id:
                db_id = self.write_to_new_records_csv(record)
            self.db_ids_in_use.add(db_id)
            csv_id_to_db_id[record.id] = db_id
        self.transfer_to_db()
        self.db_cursor.close()
        return csv_id_to_db_id

    def write_to_new_records_csv(self, record):
        new_id = self.next_id
        csv_record = self.record_to_csv(record, new_id)
        self.writer.writerow(csv_record)
        self.next_id += 1
        return new_id

    def transfer_to_db(self):
        self.outf.close()
        with open(self.tmp_ids_filename, 'w', encoding='utf8') as f:
            for a_id in self.db_ids_in_use:
                f.write('%d\n' % a_id)

        # write ids to the tmp_ids table
        self.db_cursor.execute("DELETE FROM tmp_ids")
        with open(self.tmp_ids_filename) as f:
            self.db_cursor.copy_from(f, 'tmp_ids', sep=',')
        # update agencies table with active_until
        query = """UPDATE %s SET active_until=%s 
                       WHERE active_until ISNULL AND %s NOT IN (SELECT r_id FROM tmp_ids)"""
        self.db_cursor.execute(query, (AsIs(self.db_table), self.gtfs_date, AsIs(self.db_id_field)))

        # load all new new agencies
        with open(self.records_file_name, encoding='utf8') as f:
            self.db_cursor.copy_from(f, self.db_table, sep=',', null='')
        self.db_connection.commit()


def load_agencies(db_connection, gtfs_folder, gtfs_date, tmp_folder):
    Agency = namedtuple('Agency', 'id name')

    def csv_record_supplier():
        with open(os.path.join(gtfs_folder, 'agency.txt'), encoding='utf8') as inf:
            reader = csv.DictReader(inf)
            for r in reader:
                yield Agency(int(r['agency_id']), r['agency_name'])

    cursor = db_connection.cursor()
    query = "SELECT a_id, orig_id, agency_name FROM igtfs_agencies WHERE active_until ISNULL"
    cursor.execute(query)
    db_records = {r.orginal_id: Agency(r.a_id, r.agency_name) for r in cursor.fetchall()}

    def record_matcher(record):
        original_id = record.id
        if original_id in db_records and db_records[original_id].name == record.name:
            return db_records[original_id].id

    def record_to_csv(record, new_id):
        return {'a_id': new_id,
                'orig_id': record.id,
                'agency_name': record.name,
                'active_from': gtfs_date,
                'active_until': ''}

    # noinspection PyTypeChecker
    config = LoaderConfig(csv_record_supplier=csv_record_supplier,
                          record_matcher=record_matcher,
                          record_to_csv=record_to_csv,
                          gtfs_base_file_name='agency.txt',
                          db_table='igtfs_agencies',
                          db_id_field='a_id',
                          db_fields=['a_id', 'orig_id', 'agency_name', 'active_from', 'active_until'])

    loader = Loader(db_connection, gtfs_folder, gtfs_date, tmp_folder, config)
    return loader.load()


def load_routes(db_connection, gtfs_folder, gtfs_date, tmp_folder, agency_id_map):
    Route = namedtuple('Route', 'id agency short_name long_name route_desc route_type')

    def csv_record_supplier():
        with open(os.path.join(gtfs_folder, 'routes.txt'), encoding='utf8') as inf:
            reader = csv.DictReader(inf)
            for r in reader:
                agency_id = agency_id_map[int(r['agency_id'])]
                yield Route(r['route_id'], agency_id, r['route_short_name'], r['route_long_name'],
                            r['route_desc'], r['route_type'])

    cursor = db_connection.cursor()
    query = "SELECT r_id, orig_id, agency_id, short_name, route_desc FROM igtfs_routes WHERE active_until ISNULL"
    cursor.execute(query)
    db_records = {r.orginal_id: Route(r.r_id, r.agency_id, r.route_short_name, None, r.route_desc, -1) for r in
                  cursor.fetchall()}

    def record_matcher(route):
        original_id = route.id
        current = db_records.get(original_id, None)
        if current is not None:
            if all([route.agency == current.agency, route.short_name == current.short_name,
                    route.desc == current.desc]):
                return current.r_id

    def record_to_csv(route, new_id):
        return {
            'r_id': new_id,
            'orig_id': route.id,
            'agency_id': route.agency,
            'short_name': route.short_name,
            'long_name': route.long_name,
            'route_desc': route.route_desc,
            'route_type': route.route_type,
            'active_from': gtfs_date,
            'active_until': ''
        }

    # noinspection PyTypeChecker
    config = LoaderConfig(csv_record_supplier=csv_record_supplier,
                          record_matcher=record_matcher,
                          record_to_csv=record_to_csv,
                          gtfs_base_file_name='routes.txt',
                          db_table='igtfs_routes',
                          db_id_field='r_id',
                          db_fields=['r_id', 'orig_id', 'agency_id', 'short_name', 'long_name',
                                     'route_desc', 'route_type', 'active_from', 'active_until'])

    loader = Loader(db_connection, gtfs_folder, gtfs_date, tmp_folder, config)
    return loader.load()


def load_stops(db_connection, gtfs_folder, gtfs_date, tmp_folder):
    Stop = namedtuple('Stop', ['id', 'code', 'name', 'desc', 'lat', 'lon', 'location_type',
                               'parent_station', 'zone_id'])
    gtfs_file_name = 'stops.txt'

    # stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id
    def csv_record_supplier():
        with open(os.path.join(gtfs_folder, gtfs_file_name), encoding='utf8') as inf:
            reader = csv.DictReader(inf)
            for r in reader:
                yield Stop(id=int(r['stop_id']),
                           code=int(r['stop_code']),
                           name=r['stop_name'],
                           desc=r['stop_desc'],
                           lat=r['stop_lat'],
                           lon=r['stop_lon'],
                           location_type=r['location_type'],
                           parent_station=r['parent_station'],
                           zone_id=r['zone_id'])

    cursor = db_connection.cursor()
    query = "SELECT s_id, code, name, point FROM igtfs_stops WHERE active_until ISNULL"
    cursor.execute(query)
    db_records = {r.code: (r.s_id, r.name) for r in cursor.fetchall()}

    def record_matcher(record: Stop):
        code = record.code
        if code in db_records and db_records[code][1] == record.name:
            return db_records[code][0]

    def record_to_csv(record, new_id):
        def address(stop_desc):
            return stop_desc.split(":")[1][:-4].strip()

        def town(stop_desc):
            return stop_desc.split(":")[2][:-5].strip()

        return {'s_id': new_id,
                'code': record.code,
                'name': record.name,
                's_desc': record.desc,
                'point': 'SRID=4326;POINT(%s %s)' % (record.lon, record.lat),
                'location_type': record.location_type,
                'parent_station': record.parent_station,
                'zone_id': record.zone_id,
                'address': address(record.desc),
                'town': town(record.desc),
                'active_from': gtfs_date,
                'active_until': ''}

    # noinspection PyTypeChecker
    config = LoaderConfig(csv_record_supplier=csv_record_supplier,
                          record_matcher=record_matcher,
                          record_to_csv=record_to_csv,
                          gtfs_base_file_name=gtfs_file_name,
                          db_table='igtfs_stops',
                          db_id_field='s_id',
                          db_fields=['s_id', 'code', 'name', 's_desc', 'location_type', 'parent_station',
                                     'zone_id', 'address', 'town', 'active_from', 'active_until', 'point'])

    loader = Loader(db_connection, gtfs_folder, gtfs_date, tmp_folder, config)
    return loader.load()


def load_gtfs_file(db_connection, file_name, tmp_folder):
    logging.info("Loading %s" % file_name)
    tmp_folder = os.path.join(tmp_folder, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    extract_to_folder = os.path.join(tmp_folder, 'raw')
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

    with open(os.path.join(extract_to_folder, 'calendar.txt'), encoding='utf8') as f:
        s = min(r['start_date'] for r in csv.DictReader(f))
        file_date = s[:4] + "-" + s[4:6] + "-" + s[6:]

    logging.info("File date is %s" % file_date)

    cursor = db_connection.cursor()
    insert_query = """INSERT INTO igtfs_files (file_date, file_size, imported_on) VALUES (%s, %s, current_timestamp)"""
    cursor.execute(insert_query, (file_date, os.stat(file_name).st_size))
    db_connection.commit()

    agency_id_map = load_agencies(db_connection, extract_to_folder, file_date, tmp_folder)
    load_routes(db_connection, extract_to_folder, file_date, tmp_folder, agency_id_map)
    load_stops(db_connection, extract_to_folder, file_date, tmp_folder)


conn = psycopg2.connect("dbname='obus' user='postgres' host='localhost' password='1234'")
load_gtfs_file(conn, r"C:\data\dev\transport\gtfs\2017-01-01\israel-public-transportation.zip",
               r"C:\data\dev\transport\gtfs\tmp")

conn.close()
