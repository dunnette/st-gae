from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import time
import zipfile
import StringIO
import csv
import sqlite3

class Ingestor:
    _endpoint_url = 'http://datamine.mta.info/mta_esi.php'
    _static_data_url = 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
    _sqlite_db = 'subway_status.db'
    _feed_freq = 60
    _persist_limit = 5*60
    
    def __init__(self, key_str, regen_stops = False, regen_trip_updates = False, regen_vehicles = False):
        self._key_str = key_str
        self._define_tables()
        if regen_stops:
            self._initialize_stops_table()
        if regen_trip_updates:
            self._initialize_trip_updates_table()
        if regen_vehicles:
            self._initialize_vehicles_table()
    
    def _define_tables(self):
        def wrap_text(s): return s if s else None
        self._table_definitions = {
        'trip_updates': {'name': 'trip_updates', 'n': 11, 'def': {
        0:  {'n': 'entity_id',             't': ['entity_id',             'INTEGER', 'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(e.id)}, 
        1:  {'n': 'trip_id',               't': ['trip_id',               'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(e.trip_update.trip.trip_id)}, 
        2:  {'n': 'trip_start_date',       't': ['trip_start_date',       'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(datetime.datetime.strptime(e.trip_update.trip.start_date,'%Y%m%d'))}, 
        3:  {'n': 'route_id',              't': ['route_id',              'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(e.trip_update.trip.route_id)}, 
        4:  {'n': 'stop_id',               't': ['stop_id',               'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(s.stop_id)}, 
        5:  {'n': 'direction_id',          't': ['direction_id',          'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(s.stop_id[-1])}, 
        6:  {'n': 'schedule_relationship', 't': ['schedule_relationship', 'INTEGER', 'NOT NULL'], 'f': lambda (e, s, sf): s.schedule_relationship}, 
        7:  {'n': 'arrival_time',          't': ['arrival_time',          'INTEGER'            ], 'f': lambda (e, s, sf): wrap_text(s.arrival.time)}, 
        8:  {'n': 'departure_time',        't': ['departure_time',        'INTEGER'            ], 'f': lambda (e, s, sf): wrap_text(s.departure.time)}, 
        9:  {'n': 'load_ts',               't': ['load_ts',               'INTEGER', 'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(sf._header.timestamp)}, 
        10: {'n': 'update_ts',             't': ['update_ts',             'TEXT',    'NOT NULL'], 'f': lambda (e, s, sf): wrap_text(sf._feed_update_ts)}, 
        }},
        'vehicles': {'name': 'vehicles', 'n': 1, 'def': {
        0: {'n': 'entity_id',             't': ['entity_id',             'INTEGER', 'NOT NULL'], 'f': lambda (e, s): wrap_text(e.id)},
        1: {'n': 'trip_id',               't': ['trip_id',               'TEXT',    'NOT NULL'], 'f': lambda (e, s): wrap_text(e.vehicle.trip.trip_id)},
        2: {'n': 'trip_start_date',       't': ['trip_start_date',       'TEXT',    'NOT NULL'], 'f': lambda (e, s): wrap_text(datetime.datetime.strptime(e.vehicle.trip.start_date,'%Y%m%d'))},
        3: {'n': 'route_id',              't': ['route_id',              'TEXT',    'NOT NULL'], 'f': lambda (e, s): wrap_text(e.vehicle.trip.route_id)},
        4: {'n': 'current_stop_sequence', 't': ['current_stop_sequence', 'INTEGER', 'NOT NULL'], 'f': lambda (e, s): e.vehicle.current_stop_sequence},
        5: {'n': 'current_status',        't': ['current_status',        'INTEGER', 'NOT NULL'], 'f': lambda (e, s): e.vehicle.current_status},
        6: {'n': 'status_update_time',    't': ['status_update_time',    'INTEGER', 'NOT NULL'], 'f': lambda (e, s): wrap_text(e.vehicle.timestamp)},
        7: {'n': 'load_ts',               't': ['load_ts',               'INTEGER', 'NOT NULL'], 'f': lambda (e, s): s._header.timestamp},
        8: {'n': 'update_ts',             't': ['update_ts',             'TEXT',    'NOT NULL'], 'f': lambda (e, s): s._feed_update_ts}
        }},
        'stops': {'name': 'stops', 'n': 1, 'def': {
        0:  {'n': 'stop_id',        't': ['stop_id',        'TEXT', 'NOT NULL'], 'f': lambda (r,s): wrap_text(r['stop_id'])},
        1:  {'n': 'stop_code',      't': ['stop_code',      'TEXT'            ], 'f': lambda (r,s): wrap_text(r['stop_code'])},
        2:  {'n': 'stop_name',      't': ['stop_name',      'TEXT', 'NOT NULL'], 'f': lambda (r,s): wrap_text(r['stop_name'])},
        3:  {'n': 'stop_desc',      't': ['stop_desc',      'TEXT'            ], 'f': lambda (r,s): wrap_text(r['stop_desc'])},
        4:  {'n': 'stop_lat',       't': ['stop_lat',       'REAL', 'NOT NULL'], 'f': lambda (r,s): wrap_text(r['stop_lat'])},
        5:  {'n': 'stop_lon',       't': ['stop_lon',       'REAL', 'NOT NULL'], 'f': lambda (r,s): wrap_text(r['stop_lon'])},
        6:  {'n': 'zone_id',        't': ['zone_id',        'TEXT'            ], 'f': lambda (r,s): wrap_text(r['zone_id'])},
        7:  {'n': 'stop_url',       't': ['stop_url',       'TEXT'            ], 'f': lambda (r,s): wrap_text(r['stop_url'])},
        8:  {'n': 'location_type',  't': ['location_type',  'TEXT', 'NOT NULL'], 'f': lambda (r,s): wrap_text(r['location_type'])},
        9:  {'n': 'parent_station', 't': ['parent_station', 'TEXT'            ], 'f': lambda (r,s): wrap_text(r['parent_station'])},
        10: {'n': 'update_ts',      't': ['update_ts',      'TEXT', 'NOT NULL'], 'f': lambda (r,s): s._stops_update_ts}
        }}}

    def _execute_sql(self, sql_command, arg = ''):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        cursor.execute(sql_command, arg)
        connection.commit()
        connection.close()

    def _populate_table_write(self, table_name, dataset_fields, dataset):
        sql_command = """INSERT INTO {} ('{}') VALUES ({}?);""".format(table_name, "','".join(dataset_fields),'?,'*(len(dataset_fields)-1))
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        for row in dataset:
            cursor.execute(sql_command, tuple(row))
        connection.commit()
        connection.close()

    def _create_table(self, table_name, table_structure):
        sql_command = 'CREATE TABLE {} ({});'.format(table_name,','.join([' '.join(e) for e in table_structure]))
        self._execute_sql(sql_command)

    def _drop_table(self, table_name):
        sql_command = 'DROP TABLE {};'.format(table_name)
        self._execute_sql(sql_command)

    def _initialize_feed(self, feed_id):
        self._load_feed(feed_id)
        self._split_feed()
    
    def _load_feed(self, feed_id_int):
        payload  = urllib.urlencode({'key': self._key_str, 'feed_id': feed_id_int})
        response = urllib.urlopen('{}?{}'.format(self._endpoint_url, payload))
        self._feed = gtfs_realtime_pb2.FeedMessage()
        self._feed.ParseFromString(response.read())
        
    def _split_feed(self):
        self._trip_updates = [tu for tu in self._feed.entity if tu.HasField('trip_update')]
        self._vehicles = [tu for tu in self._feed.entity if tu.HasField('vehicle')]
        self._header = self._feed.header
        
    def _initialize_stops_table(self):
        try:
            self._drop_table('stops')
        except:
            pass
        self._create_stops_table()
    
    def _create_stops_table(self):
        table_structure = [
        ['stop_id',        'TEXT', 'NOT NULL'],
        ['stop_code',      'TEXT'            ],
        ['stop_name',      'TEXT', 'NOT NULL'],
        ['stop_desc',      'TEXT'            ],
        ['stop_lat',       'REAL', 'NOT NULL'],
        ['stop_lon',       'REAL', 'NOT NULL'],
        ['zone_id',        'TEXT'            ],
        ['stop_url',       'TEXT'            ],
        ['location_type',  'TEXT', 'NOT NULL'],
        ['parent_station', 'TEXT'            ],
        ['update_ts',      'TEXT', 'NOT NULL']
        ]
        self._create_table('stops', table_structure)

    def _populate_stops_table(self):
        url = urllib.urlopen(self._static_data_url)
        f = StringIO.StringIO(url.read())
        reader = csv.DictReader(zipfile.ZipFile(f).open('stops.txt'))
        self._stops_update_ts = datetime.datetime.now()
        def wrap_text(s): return s if s else None
        dataset = [{
        'stop_id': wrap_text(row['stop_id']),
        'stop_code': wrap_text(row['stop_code']),
        'stop_name': wrap_text(row['stop_name']),
        'stop_desc': wrap_text(row['stop_desc']),
        'stop_lat': wrap_text(row['stop_lat']),
        'stop_lon': wrap_text(row['stop_lon']),
        'zone_id': wrap_text(row['zone_id']),
        'stop_url': wrap_text(row['stop_url']),
        'location_type': wrap_text(row['location_type']),
        'parent_station': wrap_text(row['parent_station']),
        'update_ts': self._stops_update_ts} for row in reader]
        dataset_fields = ['stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 
        'zone_id', 'stop_url', 'location_type', 'parent_station', 'update_ts']
        dataset_list = [[row[field] for field in dataset_fields] for row in dataset]
        self._populate_table_write('stops', dataset_fields, dataset_list)

    def update_stops_table(self):
        self._initialize_stops_table()
        self._populate_stops_table()
    
    def _initialize_vehicles_table(self):
        try:
            self._drop_table('vehicles')
        except:
            pass
        self._create_vehicles_table()
        
    def _create_vehicles_table(self):
        table_structure = [
        ['entity_id',             'INTEGER', 'NOT NULL'], 
        ['trip_id',               'TEXT',    'NOT NULL'], 
        ['trip_start_date',       'TEXT',    'NOT NULL'], 
        ['route_id',              'TEXT',    'NOT NULL'], 
        ['current_stop_sequence', 'INTEGER', 'NOT NULL'],
        ['current_status',        'INTEGER', 'NOT NULL'],
        ['status_update_time',    'INTEGER', 'NOT NULL'],
        ['load_ts',               'INTEGER', 'NOT NULL'],
        ['update_ts',             'TEXT',    'NOT NULL']
        ]
        self._create_table('vehicles',table_structure)

    def _populate_vehicles_table(self):
        def wrap_text(s): return s if s else None
        dataset = [{
        'entity_id': wrap_text(entity.id), 
        'trip_id': wrap_text(entity.vehicle.trip.trip_id),
        'trip_start_date': wrap_text(datetime.datetime.strptime(entity.vehicle.trip.start_date,'%Y%m%d')),
        'route_id': wrap_text(entity.vehicle.trip.route_id), 
        'current_stop_sequence': entity.vehicle.current_stop_sequence, 
        'current_status': entity.vehicle.current_status, 
        'status_update_time': wrap_text(entity.vehicle.timestamp),
        'load_ts': self._header.timestamp,
        'update_ts': self._feed_update_ts} for entity in self._vehicles]
        dataset_fields = ['entity_id', 'trip_id', 'trip_start_date', 'route_id', 'current_stop_sequence',
        'current_status', 'status_update_time', 'load_ts', 'update_ts']
        dataset_list = [[row[field] for field in dataset_fields] for row in dataset]
        self._populate_table_write('vehicles', dataset_fields, dataset_list)

    def _initialize_trip_updates_table(self):
        try:
            self._drop_table('trip_updates')
        except:
            pass
        self._create_trip_updates_table()
            
    def _create_trip_updates_table(self):
        table_def = self._table_definitions['trip_updates']
        table_structure = [table_def['def'][ii]['t'] for ii in range(table_def['n'])]
        self._create_table('trip_updates',table_structure)

    def _populate_trip_updates_table(self):
        table_def = self._table_definitions['trip_updates']
        dataset = [[table_def['def'][ii]['f']((entity,stu,self)) for ii in range(table_def['n'])] 
                    for entity in self._trip_updates for stu in entity.trip_update.stop_time_update]
        dataset_fields = [table_def['def'][ii]['n'] for ii in range(table_def['n'])]
        self._populate_table_write('trip_updates', dataset_fields, dataset)

    def update_feed_tables(self, feed_ids, replace = False):
        if replace:
            del self._header
            self._initialize_vehicles_table()
            self._initialize_trip_updates_table()
        if self.is_feed_stale():
            pass
        else:
            self._feed_update_ts = datetime.datetime.now()
            for feed_id in feed_ids:
                self._initialize_feed(feed_id)
                self._populate_vehicles_table()
                self._populate_trip_updates_table()
            self._clean_feed_table()

    def is_feed_stale(self):
        return hasattr(self, '_header') and time.time() - self._header.timestamp < self._feed_freq

    def _clean_feed_table(self):
        oldest_record = time.time() - self._persist_limit
        self._execute_sql('DELETE FROM trip_updates WHERE load_ts < ?', arg = (oldest_record,))
        self._execute_sql('DELETE FROM vehicles     WHERE load_ts < ?', arg = (oldest_record,))
