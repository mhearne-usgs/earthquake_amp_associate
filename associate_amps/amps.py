# stdlib imports
import os.path
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen
from urllib.parse import urlparse
from io import StringIO
import re
import json

# third party imports
import yaml
import boto3
from boto3.s3.transfer import TransferConfig
import defusedxml.cElementTree as dET
from sqlalchemy import and_
import pandas as pd
import numpy as np

# local imports
from associate_amps.amps_db import Event, Station, Channel, PGM, get_session

MAX_SIZE = 4096
# Times can have either integer or floating point (preferred) seconds
TIMEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'
ALT_TIMEFMT = '%Y-%m-%dT%H:%M:%SZ'

QUEUE_URL = 'QUEUE_URL'
DB_URL = 'DB_URL'
S3_BUCKET_URL = 'S3_BUCKET_URL'

MB = 1048576
PAYLOAD_LIMIT = 5242880

IMTS = ['acc', 'vel', 'sa', 'pga', 'pgv']
IMTDICT = {'acc': 'pga',
           'vel': 'pgv'}

# association algorithm - any peak with:
# time > origin - TMIN and time < origin + TMAX
# AND
# distance < DISTANCE
TMIN = 60
TMAX = 180
DISTANCE = 500
P_TRAVEL_TIME = 4.2

FLOAT_PATTERN = r'[-+]?[0-9]*\.?[0-9]+'

UNITS = {'PGA': '%g',
         'PGV': 'cm/s',
         'SA': '%g'
         }

#: Earth radius in km.
EARTH_RADIUS = 6371.0

# number of days after loading that amps should be deleted
MAX_AMP_AGE = 15

# number of days after origin time that earthquakes should be deleted
MAX_EVENT_AGE = 90

# number of seconds +/- to search for stations in the database
STATION_WINDOW_SECS = 10


def get_config():
    """Return queue config from url indicated by QUEUE_URL env variable.

    Returns:
        dict: Dictionary containing top level key 'queue', which contains:
              - max_process_time: Number of seconds allowed for ShakeMaps
                to complete processing.
              - max_running: Total number of ShakeMap instances to run
                simultaneously.
              - old_event_age: Seconds past which old events will be ignored.
              - future_event_age: Seconds beyond which events in the future
                will be ignored.
              - minmag: Default global magnitude threshold.
              - max_trigger_wait: Prevents event from being run too often.
              - polygons: List of dictionaries with fields:
                          - name: Name of polygon
                          - magnitude: Magnitude threshold for polygon
                          - polygon: Sequence of (X,Y) tuples.
              - repeats: Dictionary with list of dictionaries containing
                         fields:
                         - mag: Minimum magnitude threshold
                         - times: Sequence of repeat times in seconds.
              - network_delays: List of dictionaries containing fields:
                                - network: Code of network.
                                - delay: Seconds to delay processing of
                                  origins.
              - emails: Dictionary containing fields:
                        - error_emails: List of email addresses to receive
                          error messages.
                        - sender: Sender email address.

    """
    # get the environment variable telling us where our queue config is
    if QUEUE_URL not in os.environ:
        msg = (f"Could not find queue config url "
               f"environment variable {QUEUE_URL}")
        raise NameError(msg)
    queue_url = os.environ[QUEUE_URL]
    try:
        with urlopen(queue_url) as fh:
            data = fh.read().decode('utf8')

        io_obj = StringIO(data)
        config = yaml.safe_load(io_obj)

        return config
    except Exception as e:
        raise e


def insert_event(eventdict):
    """Attempt to insert an event into the ShakeMap Queue database.

    Args:
        eventdict: Event dictionary, with the following keys:
                   - eventid ('us2020abcd')
                   - netid ('us')
                   - ids (alternate id list)
                   - time (datetime in UTC)
                   - latitude
                   - longitude
                   - depth
                   - magnitude
                   - locstring
    Returns:
        bool: True if insert occurred, False if not.
    """
    if DB_URL not in os.environ:
        raise KeyError(f"Database URL {DB_URL} not in environment.")
    db_url = os.environ[DB_URL]
    session = get_session(db_url)

    # First ask if the event is already in the database
    allids = [eventdict['eventid']] + eventdict['ids']
    for eid in allids:
        eventobj = session.query(Event).filter(Event.eventid == eid).first()
        if eventobj is None:
            continue
        break

    # if we found it, update the information about the event
    if eventobj is not None:
        eventobj.eventid = eventdict['eventid']
        eventobj.time = eventdict['time']
        eventobj.lat = eventdict['latitude']
        eventobj.lon = eventdict['longitude']
        eventobj.depth = eventdict['depth']
        eventobj.magnitude = eventdict['magnitude']
        eventobj.locstring = eventdict['locstring']
        session.commit()
        session.close()
        return True

    # we didn't find it, so insert it, then die
    event = Event(eventid=eventdict['eventid'],
                  netid=eventdict['netid'],
                  time=eventdict['time'],
                  lat=eventdict['lat'],
                  lon=eventdict['lon'],
                  depth=eventdict['depth'],
                  magnitude=eventdict['magnitude'],
                  locstring=eventdict['locstring'],
                  )
    session.add(event)
    session.commit()

    session.close()


def insert_amps(ampsurl):
    """Insert data from amps file into database.
    Args:
        xmlfile (str): XML file containing peak ground motion data.
    """
    if DB_URL not in os.environ:
        raise KeyError(f"Database URL {DB_URL} not in environment.")
    db_url = os.environ[DB_URL]
    session = get_session(db_url)
    try:
        with urlopen(ampsurl) as fh:
            xmlstr = fh.read().decode('utf8')
        # sometimes these records have non-ascii bytes in them
        newxmlstr = re.sub(r'[^\x00-\x7F]+', ' ', xmlstr)
        newxmlstr = newxmlstr.encode('utf-8', errors='xmlcharrefreplace')
        amps = dET.fromstring(newxmlstr)
    except Exception as e:
        raise Exception('Could not parse %s, due to error "%s"' %
                        (ampsurl, str(e)))

    if amps.tag != 'amplitudes':
        raise Exception('%s does not appear to be an amplitude XML '
                        'file.' % ampsurl)
    agency = amps.get('agency')
    record = amps.find('record')
    timing = record.find('timing')
    reference = timing.find('reference')
    has_pgm = False
    time_dict = {}
    for child in reference.iter():
        node_name = child.tag
        if node_name == 'PGMTime':
            has_pgm = True
        elif node_name == 'year':
            time_dict['year'] = int(child.get('value'))
        elif node_name == 'month':
            time_dict['month'] = int(child.get('value'))
        elif node_name == 'day':
            time_dict['day'] = int(child.get('value'))
        elif node_name == 'hour':
            time_dict['hour'] = int(child.get('value'))
        elif node_name == 'minute':
            time_dict['minute'] = int(child.get('value'))
        elif node_name == 'second':
            time_dict['second'] = int(child.get('value'))
        elif node_name == 'msec':
            time_dict['msec'] = int(child.get('value'))
    if has_pgm:
        pgmtime_str = reference.find('PGMTime').text
        try:
            tfmt = TIMEFMT.replace('Z', '')
            pgmdate = datetime.strptime(
                pgmtime_str[0:19], tfmt).replace(tzinfo=timezone.utc)
        except ValueError:
            tfmt = ALT_TIMEFMT.replace('Z', '')
            pgmdate = datetime.strptime(
                pgmtime_str[0:19], tfmt).replace(tzinfo=timezone.utc)
    else:
        if not len(time_dict):
            print('No time data for file %s' % ampsurl)
            return
        pgmdate = datetime(time_dict['year'],
                           time_dict['month'],
                           time_dict['day'],
                           time_dict['hour'],
                           time_dict['minute'],
                           time_dict['second'])

    # there are often multiple stations per file, but they're
    # all duplicates of each other, so just grab the information
    # from the first one
    station = record.find('station')
    attrib = dict(station.items())
    lat = float(attrib['lat'])
    lon = float(attrib['lon'])
    code = attrib['code']
    name = attrib['name']
    if 'net' in attrib:
        network = attrib['net']
    elif 'netid' in attrib:
        network = attrib['netid']
    else:
        network = agency
    #
    # The station (at this pgmtime +/- 10 seconds) might already exist
    # in the DB; if it does, use it
    #
    # TODO: what is this in sqlalchemy?
    # self._cursor.execute('BEGIN EXCLUSIVE')
    minustime = pgmdate - timedelta(seconds=STATION_WINDOW_SECS)
    plustime = pgmdate + timedelta(seconds=STATION_WINDOW_SECS)
    rows = session.query(Station.id, Station.timestamp).\
        filter(Station.network == network).\
        filter(Station.code == code).\
        filter(Station.timestamp > minustime).\
        filter(Station.timestamp < plustime).all()
    #
    # It's possible that the query returned more than one station; pick
    # the one closest to the new station's pgmtime
    #
    best_sid = None
    best_time = None
    for row in rows:
        dtime = abs((row[1] - pgmdate) / timedelta(seconds=1))
        if best_time is None or dtime < best_time:
            best_time = dtime
            best_sid = row[0]
    inserted_station = False
    if best_sid is None:
        station = Station(event_id=0,
                          timestamp=pgmdate,
                          lat=lat,
                          lon=lon,
                          name=name,
                          code=code,
                          network=network,
                          loadtime=datetime.utcnow(),
                          )
        session.add(station)
        session.commit()
        best_sid = station.id
        inserted_station = True

    #
    # If the station is already there, it has at least one channel, too
    #
    existing_channels = {}
    if inserted_station is False:
        rows = session.query(Channel.channel, Channel.id).\
            filter(Station.station_id == best_sid).all()
        existing_channels = dict(rows)

    # loop over components
    channels_inserted = 0
    for channel in record.iter('component'):
        # We don't want channels with qual > 4 (assuming qual is Cosmos
        # table 6 value)
        qual = channel.get('qual')
        if qual:
            try:
                iqual = int(qual)
            except ValueError:
                # qual is something we don't understand
                iqual = 0
        else:
            iqual = 0
        if iqual > 4:
            continue
        loc = channel.get('loc')
        if not loc:
            loc = '--'
        cname = channel.get('name')
        if cname in existing_channels:
            best_cid = existing_channels[cname]
            inserted_channel = False
        else:
            channelobj = Channel(station_id=best_sid, channel=cname, loc=loc)
            session.add(channelobj)
            session.commit()
            best_cid = channelobj.id
            inserted_channel = True
            channels_inserted += 1

        #
        # Similarly, if the channel is already there, we don't want to
        # insert repeated IMTs (and updating them doesn't make a lot of
        # sense)
        #
        existing_pgms = {}
        if inserted_channel is False:
            rows = session.query(PGM.imt, PGM.id).\
                filter(PGM.channel_id == best_cid)
            existing_pgms = dict(rows)
        # loop over imts in channel
        pgm_list = []
        for pgm in list(channel):
            imt = pgm.tag
            if imt not in IMTS:
                continue
            try:
                value = float(pgm.get('value'))
            except ValueError:
                #
                # Couldn't interpret the value for some reason
                #
                continue
            if imt == 'sa':
                imt = 'p' + imt + pgm.get('period').replace('.', '')
                value = value / 9.81
            if imt in IMTDICT:
                imt = IMTDICT[imt]
            if imt == 'pga':
                value = value / 9.81
            if imt in existing_pgms:
                continue
            pgm = PGM(channel_id=best_cid, imt=imt, value=value)
            pgm_list.append(pgm)
        if len(pgm_list) > 0:
            #
            # Insert the new amps
            #
            session.bulk_save_objects(pgm_list)
            session.commit()
        elif inserted_channel:
            #
            # If we didn't insert any amps, but we inserted the channel,
            # delete the channel
            #
            session.query(Channel).filter(Channel.id == best_cid).delete()
            session.commit()
            channels_inserted -= 1
        # End of pgm loop
    # End of channel loop

    #
    # If we inserted the station but no channels, delete the station
    #
    if channels_inserted == 0 and inserted_station:
        session.query(Station).filter(Station.id == best_sid).delete()
        session.commit()
    session.close()


def associate_amps():
    if DB_URL not in os.environ:
        raise KeyError(f"Database URL {DB_URL} not in environment.")
    db_url = os.environ[DB_URL]
    session = get_session(db_url)

    # get all earthquakes
    events = session.query(Event).all()
    for event in events:
        associate(session, event)
        if len(event.stations):
            write_event_to_s3(event)
        # now delete all of the stations newly
        # associated with the event
        session.query(Station).filter(Station.event_id == event.id).delete()
        session.commit()
    session.close()


def associate(session, event):
    eqtime = event.time
    eqlat = event.lat
    eqlon = event.lon
    stime = eqtime - timedelta(seconds=TMIN)
    etime = eqtime + timedelta(seconds=TMAX)
    query = session.query(Station.id,
                          Station.network,
                          Station.name,
                          Station.code,
                          Station.timestamp,
                          Station.lat,
                          Station.lon,
                          Channel.channel,
                          PGM.imt,
                          PGM.value).with_for_update()
    query = query.join(Channel, Channel.station_id == Station.id)\
        .join(PGM, PGM.channel_id == Channel.id)
    query = query.filter(and_(Station.timestamp > stime,
                              Station.timestamp < etime))

    srows = query.all()
    cols = ['id', 'network', 'name', 'code', 'timestamp',
            'lat', 'lon', 'channel', 'imt', 'value']
    stations = pd.DataFrame(srows, columns=cols)
    stations['distance'] = geodetic_distance(eqlon, eqlat,
                                             stations['lon'],
                                             stations['lat'])

    tt = pd.to_timedelta(stations['distance'] / P_TRAVEL_TIME)
    stations['traveltime'] = tt
    eqtime_ts = pd.to_datetime(eqtime)
    tbefore = (eqtime_ts - timedelta(seconds=TMIN))
    tafter = (eqtime_ts + timedelta(seconds=TMAX))
    close_before = stations['timestamp'] > tbefore
    close_after = stations['timestamp'] < tafter
    stations['inside_time'] = close_before & close_after
    stations['inside_distance'] = stations['distance'] < DISTANCE
    stations['dt'] = ((eqtime_ts - stations['timestamp']).abs() -
                      stations['traveltime']).abs()

    # filter out stations that are outside time/distance windows
    inside_time = stations['inside_time']
    inside_distance = stations['inside_distance']
    newstations = stations[inside_time & inside_distance]

    in_expression = Station.id.in_(newstations['id'])
    udict = {Station.event_id: event.id}
    session.query(Station).\
        filter(in_expression).update(udict,
                                     synchronize_session='fetch')
    session.commit()


def clean_database():
    if DB_URL not in os.environ:
        raise KeyError(f"Database URL {DB_URL} not in environment.")
    db_url = os.environ[DB_URL]
    session = get_session(db_url)

    # delete old amps
    amp_threshold = datetime.utcnow() - timedelta(days=MAX_AMP_AGE)
    session.query(Station).filter(Station.loadtime < amp_threshold).delete()

    # delete old earthquakes
    event_threshold = datetime.utcnow() - timedelta(days=MAX_EVENT_AGE)
    session.query(Event).filter(Event.time < event_threshold).delete()

    # commit changes, and close the session
    session.commit()


def write_event_to_s3(event):
    """Write event.xml file to S3.

    Args:
        event (Event): SQLAlchemy Event object.
    """
    eventjson = get_metrics_json(event)
    transfer_config = TransferConfig(multipart_threshold=PAYLOAD_LIMIT,
                                     max_concurrency=10,
                                     multipart_chunksize=PAYLOAD_LIMIT,
                                     use_threads=True)
    s3_client = boto3.client('s3')
    key = '/'.join(['events', event.eventid, 'input', 'event.xml'])
    bucket = get_bucket()
    extra = {'ACL': 'public-read',
             'ContentType': 'text/json'}
    s3_client.upload_fileobj(eventjson, bucket, key,
                             ExtraArgs=extra,
                             Config=transfer_config)
    return True


def get_metrics_json(event):
    """Return string representation of event object.

    Args:
        event (Event): SQLAlchemy Event object.
    Returns:
        str: XML string.
    """
    tnow = datetime.utcnow().strftime(TIMEFMT)
    json_dict = {'type': 'FeatureCollection',
                 'software': {'name': 'associate_amps',
                              'version': ''},
                 'process_time': tnow,
                 'event': {'id': event.id,
                           'time': event.time.strftime(TIMEFMT),
                           'latitude': event.lat,
                           'longitude': event.lon,
                           'depth': event.depth,
                           'magnitude': event.magnitude,
                           }
                 }
    features = []
    for station in event.stations:
        feature = {'geometry': {'type': 'Point',
                                'coordinates': (station.lon, station.lat)
                                },
                   'type': 'Feature',
                   'properties': {"network_code": station.network,
                                  "station_code": station.code,
                                  "name": station.name,
                                  "provider": "NEIC ShakeMap",
                                  }
                   }

        components = {}
        for channel in station.channels:
            component = {}
            spectrals = []
            for pgm in channel.pgms:
                pgmdict = {'value': pgm.value}
                pgmname = pgm.imt.upper()
                if 'sa' in pgm.imt:
                    period = float(re.search(FLOAT_PATTERN, pgm.imt).group())
                    pgmdict['period'] = period / 10.0
                    pgmdict['damping'] = 0.05
                    pgmname = 'SA'
                    pgmdict['units'] = UNITS[pgmname]
                    spectrals.append(pgmdict)
                else:
                    pgmdict['units'] = UNITS[pgmname]
                    component[pgmname] = pgmdict
            if len(spectrals):
                component['SA'] = spectrals
            components[channel.channel] = component

        feature['properties']['components'] = components
        features.append(feature)
    json_dict['features'] = features
    jsonstr = json.dumps(json_dict)
    return jsonstr


def get_bucket():
    """Get bucket ID.

    Returns:
        str: Bucket ID suitable for use with boto3.
    """
    if S3_BUCKET_URL not in os.environ:
        raise KeyError(f"S3 Bucket URL {S3_BUCKET_URL} not in environment.")
    bucket_url = os.getenv('S3_BUCKET_URL')  # this will need to be set
    parts = urlparse(bucket_url)
    locparts = parts.netloc.split('.')
    bucket_id = locparts[0]
    return bucket_id


def start_shakemap(eventid):
    """Start ShakeMap instance with eventid.

    Args:
        eventid (str): Event ID.
    Returns:
        bool: True if startup was successful, False if not.

    """
    return True


def _prepare_coords(lons1, lats1, lons2, lats2):
    """
    Convert two pairs of spherical coordinates in decimal degrees
    to numpy arrays of radians. Makes sure that respective coordinates
    in pairs have the same shape.
    """
    lons1 = np.array(np.radians(lons1))
    lats1 = np.array(np.radians(lats1))
    assert lons1.shape == lats1.shape
    lons2 = np.array(np.radians(lons2))
    lats2 = np.array(np.radians(lats2))
    assert lons2.shape == lats2.shape
    return lons1, lats1, lons2, lats2


def geodetic_distance(lons1, lats1, lons2, lats2):
    """
    Calculate the geodetic distance between two points or two collections
    of points.

    Parameters are coordinates in decimal degrees. They could be scalar
    float numbers or numpy arrays, in which case they should "broadcast
    together".

    Implements http://williams.best.vwh.net/avform.htm#Dist

    :returns:
        Distance in km, floating point scalar or numpy array of such.
    """
    lons1, lats1, lons2, lats2 = _prepare_coords(lons1, lats1, lons2, lats2)
    distance = np.arcsin(np.sqrt(
        np.sin((lats1 - lats2) / 2.0) ** 2.0
        + np.cos(lats1) * np.cos(lats2)
        * np.sin((lons1 - lons2) / 2.0) ** 2.0
    ).clip(-1., 1.))
    return (2.0 * EARTH_RADIUS) * distance
