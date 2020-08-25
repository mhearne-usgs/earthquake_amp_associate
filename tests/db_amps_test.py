#!/usr/bin/env python

from datetime import datetime, timedelta
import tempfile
import shutil
import pathlib
import os.path
import sys

# third party imports
from numpy.testing import assert_almost_equal
from sqlalchemy import inspect

from associate_amps.amps_db import (Event, Station,
                                    Channel, PGM,
                                    get_session, Base)


def test_amps_db(user=None, password=None, host=None):
    try:
        if user is not None:
            dburl = f'mysql+pymysql://{user}:{password}@{host}/amps'
            session = get_session(dburl)
        else:
            session = get_session()
        t1 = datetime(2020, 8, 21)
        event = Event(eventid='us2020abcd',
                      netid='us',
                      time=t1,
                      lat=32.123,
                      lon=-118.123,
                      depth=10.1,
                      magnitude=5.6,
                      locstring='somewhere in california')
        session.add(event)
        session.commit()
        assert str(event) == 'Event: us2020abcd'
        age_in_days = (datetime.utcnow() - t1) / timedelta(days=1)
        assert_almost_equal(event.age_in_days, age_in_days, decimal=1)
        station = Station(event_id=event.id,
                          timestamp=datetime(2020, 8, 21, 0, 0, 30),
                          lat=32.456,
                          lon=-118.456,
                          network='ci',
                          name='Station 1',
                          code='ABCD',
                          loadtime=datetime.utcnow())
        session.add(station)
        session.commit()
        assert str(station) == 'Station: ABCD, Station 1'
        channel = Channel(station_id=station.id,
                          channel='HNE',
                          loc='01')
        session.add(channel)
        session.commit()
        assert str(channel) == 'Channel: HNE'
        pgm = PGM(channel_id=channel.id,
                  imt='PGA',
                  value=1.0)
        session.add(pgm)
        session.commit()
        assert str(pgm) == 'PGM: PGA = 1.0'

        # now test relationships
        assert len(event.stations) == 1
        assert len(station.channels) == 1
        assert len(channel.pgms) == 1

        # test counts
        assert session.query(Station).count() == 1
        assert session.query(Channel).count() == 1
        assert session.query(PGM).count() == 1

        # now test cascading deletes
        # this should delete all channels, which should trigger pgm deletes as well
        session.delete(station)
        session.commit()

        assert session.query(Station).count() == 0
        assert session.query(Channel).count() == 0
        assert session.query(PGM).count() == 0
    except Exception as e:
        raise(e)
    finally:
        engine = session.get_bind()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f'Dropping all tables {tables}')
        session.commit()
        # this deletes the tables from the database
        Base.metadata.drop_all(bind=engine)

    session.close()


def test_delete_file(user=None, password=None, host=None):
    try:
        tdir = tempfile.mkdtemp()
        if user is not None:
            dburl = f'mysql+pymysql://{user}:{password}@{host}/amps'
        else:
            dbfile = pathlib.Path(tdir) / 'test.db'
            dburl = dbfile.as_uri().replace('file:', 'sqlite:/')
        os.environ['DB_URL'] = dburl
        session = get_session(dburl)
        station = Station(event_id=None,
                          timestamp=datetime(2020, 8, 21, 0, 0, 30),
                          lat=32.456,
                          lon=-118.456,
                          network='ci',
                          name='Station 1',
                          code='ABCD',
                          loadtime=datetime.utcnow())
        session.add(station)
        session.commit()
        assert str(station) == 'Station: ABCD, Station 1'
        channel = Channel(station_id=station.id,
                          channel='HNE',
                          loc='01')
        session.add(channel)
        session.commit()
        assert str(channel) == 'Channel: HNE'
        pgm = PGM(channel_id=channel.id,
                  imt='PGA',
                  value=1.0)
        session.add(pgm)
        session.commit()

        # now try deleting things
        session.delete(station)
        session.commit()

        # now count things
        assert session.query(Station).count() == 0
        assert session.query(Channel).count() == 0
        assert session.query(PGM).count() == 0

        session.close()
    except Exception as e:
        raise(e)
    finally:
        engine = session.get_bind()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f'Dropping all tables {tables}')
        session.commit()
        # this deletes the tables from the database
        Base.metadata.drop_all(bind=engine)
        shutil.rmtree(tdir)


if __name__ == '__main__':
    host = None
    user = None
    password = None
    if len(sys.argv) == 4:
        host = sys.argv[1]
        user = sys.argv[2]
        password = sys.argv[3]

    test_amps_db(host=host, user=user, password=password)
    test_delete_file(host=host, user=user, password=password)
