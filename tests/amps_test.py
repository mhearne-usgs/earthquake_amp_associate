#!/usr/bin/env python

from datetime import datetime
import shutil
import tempfile
import pathlib
import os.path
import json
from unittest import mock

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
              'network': '',
              'time': datetime(2018, 3, 7, 18, 5, 0),
              'lat': 35.487,
              'lon': -120.027,
              'depth': 8.0,
              'locstring': 'Somewhere in California',
              'magnitude': 3.7}
    try:
        tdir = tempfile.mkdtemp()
        dbfile = pathlib.Path(tdir) / 'test.db'
        # dbfile.touch()
        dburl = dbfile.as_uri().replace('file:', 'sqlite:/')
        os.environ['DB_URL'] = dburl
        insert_event(event1)
        root = pathlib.Path(__file__).parent
        xmlfile1 = (root / 'data' / 'USR_100416_20180307_180450.xml').as_uri()
        xmlfile2 = (root / 'data' /
                    'USR_100416_20180307_180450_2.xml').as_uri()
        xmlfile3 = (root / 'data' /
                    'USR_100416_20180307_180450_3.xml').as_uri()
        xmlfile4 = (root / 'data' /
                    'USR_100416_20180307_180450_4.xml').as_uri()
        xmlfile5 = (root / 'data' /
                    'USR_100416_20180307_180450_5.xml').as_uri()
        xmlfile6 = (root / 'data' /
                    'USR_100416_20180307_180450_6.xml').as_uri()

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
        udict = {Station.event_id: 0}
        session.query(Station).\
            filter(Station.event_id == event.id).\
            update(udict, synchronize_session='fetch')
        session.commit()
        session.close()

        # now test the associate algorithm
        os.environ['S3_BUCKET_URL'] = 'foo'
        with mock.patch('boto3.client', return_value=MockS3Client()) as _:
            associate_amps()

    except Exception as e:
        print(e)
    finally:
        shutil.rmtree(tdir)


if __name__ == '__main__':
    test_amps()
