import psycopg2
from psycopg2.extensions import AsIs
import csv
from collections import namedtuple
import os
import zipfile
import datetime
import logging
from abc import ABC, abstractmethod


class Loader(ABC):
    def __init__(self, db_connection, gtfs_folder, gtfs_date, tmp_folder):
        self.db_connection = db_connection
        self.gtfs_folder = gtfs_folder
        self.gtfs_date = gtfs_date
        self.tmp_folder = tmp_folder
        self.db_ids_in_use = set()
        self.db_cursor = db_connection.cursor()

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

    @abstractmethod
    def csv_record_supplier(self):
        return []

    @abstractmethod
    def record_matcher(self, record):
        pass

    @abstractmethod
    def write_to_new_records_csv(self, record):
        pass

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
        self.db_cursor.execute(query, (AsIs(self.db_table()), self.gtfs_date, AsIs(self.id_column())))

        # load all new new agencies
        with open(self.records_file_name, encoding='utf8') as f:
            self.db_cursor.copy_from(f, self.db_table(), sep=',', null='')
        self.db_connection.commit()

    @abstractmethod
    def db_table(self):
        pass

    @abstractmethod
    def id_column(self):
        pass


class AgenciesLoader(Loader):
    Agency = namedtuple('Agency', 'id name')

    def __init__(self, *args, **kwargs):
        super(AgenciesLoader, self).__init__(*args, **kwargs)
        self.input_file_name = os.path.join(self.gtfs_folder, 'agency.txt')
        self.records_file_name = os.path.join(self.tmp_folder, 'agency.txt')
        self.tmp_ids_filename = os.path.join(self.tmp_folder, 'agency_ids_in_use.txt')

        query = "SELECT a_id, orig_id, agency_name FROM igtfs_agencies WHERE active_until ISNULL"
        self.db_cursor.execute(query)
        self.db_records = {r.orginal_id: AgenciesLoader.Agency(r.a_id, r.agency_name)
                           for r in self.db_cursor.fetchall()}

        self.outf = open(self.records_file_name, 'w', encoding='utf8')
        self.writer = csv.DictWriter(self.outf, ['a_id', 'orig_id', 'agency_name', 'active_from', 'active_until'],
                                     lineterminator='\n')
        # self.writer.writeheader()
        # noinspection PyTypeChecker
        self.next_id = max(self.db_records) + 1 if len(self.db_records) > 0 else 1

    def db_table(self):
        return 'igtfs_agencies'

    def id_column(self):
        return 'a_id'

    def csv_record_supplier(self):
        with open(self.input_file_name, encoding='utf8') as inf:
            reader = csv.DictReader(inf)
            for r in reader:
                yield AgenciesLoader.Agency(int(r['agency_id']), r['agency_name'])

    def record_matcher(self, record):
        original_id = record.id
        if original_id in self.db_records and self.db_records[original_id].name == record.name:
            return self.db_records[original_id].id

    def write_to_new_records_csv(self, record):
        new_id = self.next_id
        self.writer.writerow({
            'a_id': new_id,
            'orig_id': record.id,
            'agency_name': record.name,
            'active_from': self.gtfs_date,
            'active_until': ''
        })
        self.next_id += 1
        return new_id



def load_agencies(db_connection, gtfs_folder, gtfs_date, tmp_folder):
    loader = AgenciesLoader(db_connection, gtfs_folder, gtfs_date, tmp_folder)
    return loader.load()


class RoutesLoader(Loader):
    Route = namedtuple('Route', 'id agency short_name long_name route_desc route_type')

    def __init__(self, agency_id_map, *args, **kwargs):
        super(RoutesLoader, self).__init__(*args, **kwargs)
        self.agency_id_map = agency_id_map
        self.input_file_name = os.path.join(self.gtfs_folder, 'routes.txt')
        self.records_file_name = os.path.join(self.tmp_folder, 'routes.txt')

        query = "SELECT r_id, orig_id, agency_id, short_name, route_desc FROM igtfs_routes WHERE active_until ISNULL"
        self.db_cursor.execute(query)
        self.db_records = {r.orginal_id: RoutesLoader.Route(r.r_id, r.agency_id, r.route_short_name,
                                                            None, r.route_desc, -1) for r in
                           self.db_cursor.fetchall()}

        self.outf = open(self.records_file_name, 'w', encoding='utf8')
        self.writer = csv.DictWriter(self.outf, ['r_id', 'orig_id', 'agency_id', 'short_name', 'long_name',
                                                 'route_desc', 'route_type', 'active_from', 'active_until'],
                                     lineterminator='\n')
        # self.writer.writeheader()
        # noinspection PyTypeChecker
        self.next_id = max(self.db_records) + 1 if len(self.db_records) > 0 else 1
        self.tmp_ids_filename = os.path.join(self.tmp_folder, 'route_ids_in_use.txt')

    def csv_record_supplier(self):
        with open(self.input_file_name, encoding='utf8') as inf:
            reader = csv.DictReader(inf)
            for r in reader:
                agency_id = self.agency_id_map[int(r['agency_id'])]
                yield RoutesLoader.Route(r['route_id'], agency_id, r['route_short_name'], r['route_long_name'],
                                         r['route_desc'], r['route_type'])

    def record_matcher(self, route):
        original_id = route.id
        if original_id not in self.db_records:
            return None
        current = self.db_records[original_id]
        if all([route.agency == current.agency, route.short_name == current.short_name, route.desc == current.desc]):
            return current.r_id
        return None

    def write_to_new_records_csv(self, route):
        new_id = self.next_id
        self.writer.writerow({
            'r_id': self.next_id,
            'orig_id': route.id,
            'short_name': route.short_name,
            'long_name': route.long_name,
            'route_desc': route.route_desc,
            'route_type': route.route_type,
            'active_from': self.gtfs_date,
            'active_until': ''
        })
        self.next_id += 1
        return new_id

    def db_table(self):
        return 'igtfs_routes'

    def id_column(self):
        return 'r_id'


def load_routes(db_connection, gtfs_date, routes_file, tmp_folder, agency_id_map):
    loader = RoutesLoader(agency_id_map, db_connection, gtfs_date, routes_file, tmp_folder)
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


conn = psycopg2.connect("dbname='obus' user='postgres' host='localhost' password='1234'")
load_gtfs_file(conn, r"C:\data\dev\transport\gtfs\2017-01-01\israel-public-transportation.zip",
               r"C:\data\dev\transport\gtfs\tmp")

conn.close()
