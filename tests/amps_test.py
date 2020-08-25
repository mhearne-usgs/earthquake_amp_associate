#!/usr/bin/env python

from datetime import datetime, timedelta
import shutil
import tempfile
import pathlib
import os.path
import json
from unittest import mock
import time

from associate_amps.amps_db import Event, Station, Channel, PGM, get_session
from associate_amps.amps import (insert_event,
                                 insert_amps,
                                 get_metrics_json,
                                 associate,
                                 associate_amps,
                                 clean_database,
                                 TIMEFMT)


class MockS3Client(object):
    """Mock boto3 S3 client"""

    def upload_fileobj(self, eventxml, bucket, key,
                       ExtraArgs=None,
                       Config=None):
        return None


def test_amps():
    event1 = {'eventid': 'ci37889959',
              'ids': ['us2018abcd'],
              'netid': 'ci',
              'time': datetime(2018, 3, 7, 18, 5, 0),
              'lat': 35.487,
              'lon': -120.027,
              'depth': 8.0,
              'locstring': 'Somewhere in California',
              'magnitude': 3.7}
    try:
        tdir = tempfile.mkdtemp()
        dbfile = pathlib.Path(tdir) / 'test.db'
        dburl = dbfile.as_uri().replace('file:', 'sqlite:/')
        os.environ['DB_URL'] = dburl
        insert_event(event1)
        root = pathlib.Path(__file__).absolute().parent
        xmlfile1 = (root / 'data' / 'USR_100416_20180307_180450.xml').as_uri()
        insert_amps(xmlfile1)

        # test to ensure the right stuff was inserted
        session = get_session(dburl)
        assert session.query(Station).count() == 1
        assert session.query(Channel).count() == 3
        assert session.query(PGM).count() == 15
        pgms = [pgm[0] for pgm in session.query(PGM.imt).distinct().all()]
        assert sorted(pgms) == ['pga', 'pgv', 'psa03', 'psa10', 'psa30']

        # test the low-level associate algorithm
        event = session.query(Event).first()
        associate(session, event)
        assert len(event.stations) == 1
        # now test the code that makes the json
        jsonstr = get_metrics_json(event)
        jdict = json.loads(jsonstr)
        cmpkeys = ['event', 'features', 'process_time', 'software', 'type']
        assert sorted(jdict.keys()) == cmpkeys
        assert len(jdict['features']) == 1
        assert len(jdict['features'][0]['properties']['components']) == 3
        # assert jsonstr == CMPSTR
        # undo the association
        udict = {Station.event_id: None}
        session.query(Station).\
            filter(Station.event_id == event.id).\
            update(udict, synchronize_session='fetch')
        session.commit()
        session.close()

        # now test the associate algorithm
        os.environ['S3_BUCKET_URL'] = 'foo'
        with mock.patch('boto3.client', return_value=MockS3Client()) as _:
            associate_amps()

        # now test the cleaning algorithm
        insert_amps(xmlfile1)
        time.sleep(4)
        session = get_session(dburl)
        assert session.query(Station).count() == 1
        assert session.query(Channel).count() == 3
        assert session.query(PGM).count() == 15
        session.close()
        mock_max_amp_age = 2 / 86400  # very small fraction of a day
        mock_max_event_age = 3 / 86400  # very small fraction of a day
        mock1 = 'associate_amps.amps.MAX_AMP_AGE'
        mock2 = 'associate_amps.amps.MAX_EVENT_AGE'
        with mock.patch(mock1, mock_max_amp_age) as _, \
                mock.patch(mock2, mock_max_event_age) as _:
            clean_database()
        session = get_session(dburl)
        assert session.query(Station).count() == 0
        assert session.query(Channel).count() == 0
        assert session.query(PGM).count() == 0
        assert session.query(Event).count() == 0
        session.close()

    except Exception as e:
        raise(e)
    finally:
        shutil.rmtree(tdir)


def make_station_xml(station):
    station_str = f'''<?xml version="1.0" encoding="US-ASCII" standalone="yes"?>
<amplitudes agency="{station['network']}">
<record> 
<timing> 
  <reference zone="GMT" quality="0.5">
    <PGMTime>{station['timestamp'].strftime(TIMEFMT)}</PGMTime>
  </reference>
  <trigger value="0"/>
</timing>
<station code="{station['code']}" net="{station['network']}"
          lat="{station['lat']}" lon="{station['lon']}"
          name="{station['name']}">
<component name="HN1" loc="05" qual="3">
  <pga value="0.0315"  units="cm/s/s" datetime="2018-03-07T18:04:52.925Z"/>
  <pgv value="0.00297"  units="cm/s"   datetime="2018-03-07T18:05:59.605Z"/>
  <pgd value="0.001932"  units="cm"     datetime="2018-03-07T18:05:59.065Z"/>
  <sa period="0.3" value="0.024930" units="cm/s/s"
      datetime="2018-03-07T18:05:10.360Z"/>
  <sa period="1.0" value="0.016455" units="cm/s/s"
      datetime="2018-03-07T18:06:05.225Z"/>
  <sa period="3.0" value="0.009941" units="cm/s/s"
      datetime="2018-03-07T18:05:56.275Z"/>
</component>
<component name="HN2" loc="05" qual="3">
  <pga value="0.0315"  units="cm/s/s" datetime="2018-03-07T18:04:52.925Z"/>
  <pgv value="0.00297"  units="cm/s"   datetime="2018-03-07T18:05:59.605Z"/>
  <pgd value="0.001932"  units="cm"     datetime="2018-03-07T18:05:59.065Z"/>
  <sa period="0.3" value="0.024930" units="cm/s/s" 
      datetime="2018-03-07T18:05:10.360Z"/>
  <sa period="1.0" value="0.016455" units="cm/s/s" 
      datetime="2018-03-07T18:06:05.225Z"/>
  <sa period="3.0" value="0.009941" units="cm/s/s" 
      datetime="2018-03-07T18:05:56.275Z"/>
</component>
<component name="HNZ" loc="05" qual="3">
  <pga value="0.0315"  units="cm/s/s" datetime="2018-03-07T18:04:52.925Z"/>
  <pgv value="0.00297"  units="cm/s"   datetime="2018-03-07T18:05:59.605Z"/>
  <pgd value="0.001932"  units="cm"     datetime="2018-03-07T18:05:59.065Z"/>
  <sa period="0.3" value="0.024930" units="cm/s/s" 
      datetime="2018-03-07T18:05:10.360Z"/>
  <sa period="1.0" value="0.016455" units="cm/s/s" 
      datetime="2018-03-07T18:06:05.225Z"/>
  <sa period="3.0" value="0.009941" units="cm/s/s" 
      datetime="2018-03-07T18:05:56.275Z"/>
</component>
</station>
</record>
</amplitudes>
'''
    return station_str


def test_duplicates():
    t1 = datetime(2020, 1, 1)
    t2 = t1 + timedelta(seconds=30)
    event1 = {'eventid': 'us2020abcd',
              'netid': 'us',
              'ids': [],
              'time': t1,
              'lat': 0.0,
              'lon': 0.0,
              'depth': 10.0,
              'magnitude': 6.5,
              'locstring': 'somewhere',
              }
    event2 = {'eventid': 'us2020efgh',
              'netid': 'us',
              'ids': [],
              'time': t2,
              'lat': 0.5,
              'lon': 0.5,
              'depth': 10.0,
              'magnitude': 6.5,
              'locstring': 'somewhere',
              }
    station1 = {'code': 'ABC',
                'timestamp': t1 - timedelta(seconds=30),
                'lat': 0.25,
                'lon': 0.25,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }

    station2 = {'code': 'DEF',
                'timestamp': t2 + timedelta(seconds=30),
                'lat': 0.35,
                'lon': 0.35,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }
    station3 = {'code': 'ABC',
                'timestamp': t2 + timedelta(seconds=40),
                'lat': 0.35,
                'lon': 0.35,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }
    station4 = {'code': 'DEF',
                'timestamp': t2 + timedelta(seconds=15),
                'lat': 0.35,
                'lon': 0.35,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }
    # station5 should not pass the time check
    station5 = {'code': 'GHI',
                'timestamp': t2 + timedelta(seconds=800),
                'lat': 0.30,
                'lon': 0.30,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }
    # station6 should not pass the distance check
    station6 = {'code': 'JKL',
                'timestamp': t2 - timedelta(seconds=15),
                'lat': 6.0,
                'lon': 6.0,
                'network': 'us',
                'name': 'Station 1',
                'event_id': 0,
                }

    try:
        tdir = tempfile.mkdtemp()
        dbfile = pathlib.Path(tdir) / 'test.db'
        # dbfile.touch()
        dburl = dbfile.as_uri().replace('file:', 'sqlite:/')
        os.environ['DB_URL'] = dburl
        insert_event(event1)
        insert_event(event2)
        stations = [station1, station2, station3, station4, station5, station6]
        for station in stations:
            station_str = make_station_xml(station)
            sfile = pathlib.Path(tdir) / 'tmpstation.xml'
            with open(sfile, 'wt') as f:
                f.write(station_str)
            station_url = sfile.as_uri()
            insert_amps(station_url)

        # check out our database, associate with one event
        session = get_session(dburl)
        assert session.query(Event).count() == 2
        assert session.query(Station).count() == 6
        session.close()
        event = session.query(Event).\
            filter(Event.eventid == 'us2020abcd').first()
        associate(session, event)

        # check to see we got the right stations associated,
        # and the duplicate stations deleted
        session = get_session(dburl)
        event = session.query(Event).\
            filter(Event.eventid == 'us2020abcd').first()
        assert len(event.stations) == 2
        cmplist = [(1, 'ABC'), (1, 'DEF'), (None, 'GHI'), (None, 'JKL')]
        slist = session.query(Station.event_id, Station.code).all()
        assert slist == cmplist
        session.close()

        os.environ['S3_BUCKET_URL'] = 'foo'
        with mock.patch('boto3.client', return_value=MockS3Client()) as _:
            associate_amps()
    except Exception as e:
        raise(e)
    finally:
        shutil.rmtree(tdir)


def test_update_event():
    t1 = datetime(2020, 8, 24)
    t2 = t1 + timedelta(seconds=34)
    event1 = {'eventid': 'ci12345678',
              'netid': 'us',
              'ids': [],
              'time': t1,
              'lat': 0.0,
              'lon': 0.0,
              'depth': 10.0,
              'magnitude': 6.5,
              'locstring': 'somewhere',
              }
    event2 = {'eventid': 'us2020abcd',
              'netid': 'us',
              'ids': ['ci12345678'],
              'time': t2,
              'lat': 0.1,
              'lon': 0.5,
              'depth': 10.0,
              'magnitude': 6.7,
              'locstring': 'somewhere',
              }
    try:
        tdir = tempfile.mkdtemp()
        dbfile = pathlib.Path(tdir) / 'test.db'
        # dbfile.touch()
        dburl = dbfile.as_uri().replace('file:', 'sqlite:/')
        os.environ['DB_URL'] = dburl
        insert_event(event1)
        session = get_session(dburl)
        tmpevent = session.query(Event).first()
        assert tmpevent.magnitude == 6.5
        session.close()
        insert_event(event2)
        session = get_session(dburl)
        tmpevent = session.query(Event).first()
        assert tmpevent.magnitude == 6.7
        assert tmpevent.eventid == 'us2020abcd'
        assert tmpevent.time == t2
        session.close()
    except Exception as e:
        raise(e)

    session.close()


if __name__ == '__main__':
    test_update_event()
    test_duplicates()
    test_amps()
