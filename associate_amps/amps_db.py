# stdlib imports
import logging
from datetime import datetime, timedelta

# third party imports
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, Float, String,
                        DateTime, ForeignKey, Boolean)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy_utils import database_exists, create_database

# We dynamically (not sure why?) create the base class for our objects
Base = declarative_base()

TIMEFMT = '%Y-%m-%dT%H:%M:%S'

MYSQL_TIMEOUT = 30

# association algorithm - any peak with:
# time > origin - TMIN and time < origin + TMAX
# AND
# distance < DISTANCE
TMIN = 60
TMAX = 180
DISTANCE = 500
P_TRAVEL_TIME = 4.2


class IncorrectDataTypesException(Exception):
    pass


class IncompleteConstructorException(Exception):
    pass


def get_session(url='sqlite:///:memory:', create_db=True):
    """Get a SQLAlchemy Session instance for input database URL.
    :param url:
      SQLAlchemy URL for database, described here:
        http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls.
    :param create_db:
      Boolean indicating whether to create database from scratch.
    :returns:
      Sqlalchemy Session instance.
    """
    # Create a sqlite in-memory database engine
    if not database_exists(url):
        if create_db:
            create_database(url)
        else:
            msg = ('Database does not exist, will not create without '
                   'create_db turned on.')
            logging.error(msg)
            return None

    connect_args = {}
    if 'mysql' in url.lower():
        connect_args = {'connect_timeout': MYSQL_TIMEOUT}

    engine = create_engine(url, echo=False, connect_args=connect_args)
    Base.metadata.create_all(engine)

    # create a session object that we can use to insert and
    # extract information from the database
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    return session


class Event(Base):
    """Class representing the "event" table in the database.

    """
    EVENT = {'eventid': String(64),
             'netid': String(32),
             'time': DateTime(),
             'lat': Float(),
             'lon': Float(),
             'depth': Float(),
             'magnitude': Float(),
             'locstring': String(1024),
             }
    __tablename__ = 'event'
    id = Column(Integer, primary_key=True)
    eventid = Column(EVENT['eventid'], index=True)
    netid = Column(EVENT['netid'])
    time = Column(EVENT['time'])
    lat = Column(EVENT['lat'])
    lon = Column(EVENT['lon'])
    depth = Column(EVENT['depth'])
    magnitude = Column(EVENT['magnitude'])
    locstring = Column(EVENT['locstring'])

    stations = relationship("Station", back_populates="event",
                            cascade="all, delete, delete-orphan")

    @property
    def is_running(self):
        for queue in self.queued:
            if queue.is_running:
                return True
        return False

    @property
    def age_in_days(self):
        return (datetime.utcnow() - self.time) / timedelta(days=1)

    def __init__(self, **kwargs):
        """Instantiate an Event object from scratch (i.e., not from a query).

        Note: Although keyword arguments, all arguments below must be supplied.

        Args:
            eventid (str): Event ID of the form "us2020abcd".
            netid (str): The network code at the beginning of the eventid.
            time (datetime): Origin time, UTC.
            lat (float): Origin latitude.
            lon (float): Origin longitude.
            depth (float): Origin depth.
            magnitude (float): Origin magnitude.
            locstring (str): Description of earthquake location.
            lastrun (datetime): Set this to something like datetime(1900,1,1).

        Returns:
            Event: Instance of the Event object.
        """
        validate_inputs(self.EVENT, kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return (f'Event: {self.eventid}')


class Station(Base):
    STATION = {'event_id': Integer(),
               'timestamp': DateTime(),
               'lat': Float(),
               'lon': Float(),
               'network': String(32),
               'name': String(1024),
               'code': String(32),
               'loadtime': DateTime(),
               }
    __tablename__ = 'station'
    id = Column(Integer, primary_key=True)
    event_id = Column(STATION['event_id'], ForeignKey('event.id'))
    timestamp = Column(STATION['timestamp'])
    lat = Column(STATION['lat'])
    lon = Column(STATION['lon'])
    network = Column(STATION['network'], index=True)
    name = Column(STATION['name'])
    code = Column(STATION['code'], index=True)
    loadtime = Column(STATION['loadtime'])

    # a station can have one event
    event = relationship("Event", back_populates='stations')

    # a station can have many channels
    channels = relationship('Channel', back_populates='station',
                            cascade="all, delete, delete-orphan")

    def __init__(self, **kwargs):
        validate_inputs(self.STATION, kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return (f'Station: {self.code}, {self.name}')


class Channel(Base):
    CHANNEL = {'station_id': Integer(),
               'channel': String(32),
               'loc': String(1024),
               }
    __tablename__ = 'channel'
    id = Column(Integer, primary_key=True)
    station_id = Column(CHANNEL['station_id'],
                        ForeignKey('station.id'), index=True)
    channel = Column(CHANNEL['channel'])
    loc = Column(CHANNEL['loc'])

    # a channel has one station that it belongs to
    station = relationship("Station", back_populates='channels')

    # a channel has many pgms
    pgms = relationship("PGM", back_populates='channel',
                        cascade="all, delete, delete-orphan")

    def __init__(self, **kwargs):
        validate_inputs(self.CHANNEL, kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return (f'Channel: {self.channel}')


class PGM(Base):
    PGM = {'channel_id': Integer(),
           'imt': String(16),
           'value': Float(),
           }
    __tablename__ = 'pgm'
    id = Column(Integer, primary_key=True)
    channel_id = Column(PGM['channel_id'],
                        ForeignKey('channel.id'), index=True)
    imt = Column(PGM['imt'])
    value = Column(PGM['value'])

    # a channel has one station that it belongs to
    channel = relationship("Channel", back_populates='pgms')

    def __init__(self, **kwargs):
        validate_inputs(self.PGM, kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return (f'PGM: {self.imt} = {self.value}')


def validate_inputs(defdict, kwdict):
    """Validate all init() inputs against the python types of table columns.

    Args:
        defdict (dict): Dictionary containing the column
                        names/SQLAlchemy types.
        kwdict (dict): Dictionary containing the init() kwargs.

    Raises:
        IncompleteConstructorException: Not all kwargs are set.
        IncorrectDataTypesException: At least one of the kwargs is
                                     of the wrong type.
    """
    # first check that all required parameters are being set
    if not set(defdict.keys()) <= set(kwdict.keys()):
        msg = ('In Event constructor, all the following values must be set:'
               f'{str(list(defdict.keys()))}')
        raise IncompleteConstructorException(msg)

    errors = []
    for key, value in kwdict.items():
        ktype = defdict[key].python_type
        if not isinstance(value, ktype):
            errors.append(f'{key} must be of type {ktype}')
    if len(errors):
        msg = '\n'.join(errors)
        raise IncorrectDataTypesException(msg)
