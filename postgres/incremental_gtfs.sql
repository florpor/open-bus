CREATE TABLE igtfs_files
(
  file_date   DATE      NOT NULL, -- earliest start_date in calendar file
  file_size   INTEGER   NOT NULL,
  imported_on TIMESTAMP NOT NULL, -- timestamp of record insert
  CONSTRAINT igtfs_files_pkey PRIMARY KEY (file_date)
);


CREATE TABLE igtfs_agencies
(
  a_id         INTEGER                NOT NULL, -- internal agency id
  orig_id      INTEGER                NOT NULL, -- original agency id from gtfs file
  agency_name  CHARACTER VARYING(100) NOT NULL,
  active_from  DATE                   NOT NULL REFERENCES igtfs_files (file_date),
  active_until DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_agency_pkey PRIMARY KEY (a_id)
);


CREATE TABLE igtfs_routes
(
  r_id         INTEGER NOT NULL,
  orig_id      INTEGER NOT NULL,
  agency_id    INTEGER REFERENCES igtfs_agencies (a_id),
  short_name   CHARACTER VARYING(50),
  long_name    CHARACTER VARYING(255),
  route_desc   CHARACTER VARYING(10),
  route_type   INTEGER NOT NULL,
  active_from  DATE    NOT NULL REFERENCES igtfs_files (file_date),
  active_until DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_routes_pkey PRIMARY KEY (r_id)
);

CREATE TABLE igtfs_stops
(
  s_id           INTEGER NOT NULL,
  code           INTEGER NOT NULL,
  name           CHARACTER VARYING(255),
  s_desc         CHARACTER VARYING(255),
  location_type  BOOLEAN,
  parent_station INTEGER,
  zone_id        CHARACTER VARYING(255),
  address        CHARACTER VARYING(50),
  town           CHARACTER VARYING(50),
  active_from    DATE    NOT NULL REFERENCES igtfs_files (file_date),
  active_until   DATE REFERENCES igtfs_files (file_date),
  CONSTRAINT igtfs_stops_pkey PRIMARY KEY (s_id)
);


SELECT AddGeometryColumn ('igtfs_stops','point',4326,'POINT',2);


CREATE TABLE tmp_ids
(
  r_id INTEGER NOT NULL,
  CONSTRAINT tmp_ids_pkey PRIMARY KEY (r_id)
);