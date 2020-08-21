#!/usr/bin/env python

from datetime import datetime

from associate_amps.amps_db import Event, Station, Channel, PGM, get_session


def test_amps_db():
    session = get_session()

    event = Event(eventid='us2020abcd',
                  netid='us',
                  time=datetime(2020, 8, 21),
                  lat=32.123,
                  lon=-118.123,
                  depth=10.1,
                  magnitude=5.6,
                  locstring='somewhere in california')
    session.add(event)
    session.commit()
    assert str(event) == 'Event: us2020abcd'
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
    session.close()


if __name__ == '__main__':
    test_amps_db()
