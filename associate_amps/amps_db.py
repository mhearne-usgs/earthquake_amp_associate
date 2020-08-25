# stdlib imports
import logging
from datetime import datetime, timedelta

# third party imports
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, Float, String,
                        DateTime, ForeignKey, Boolean)
from sqlalchemy import create_engine, event
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


def _fk_pragma_on_connect(dbapi_con, con_record):
    # use of this ensures that foreign key delete cascade
    # directives will be obeyed, at least in sqlite.
    dbapi_con.execute('pragma foreign_keys=ON')


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
    if 'sqlite' in url:
        # make sure that we enable foreign keys when using sqlite
        event.listen(engine, 'connect', _fk_pragma_on_connect)
    Base.metadata.create_all(engine)

    # create a session object that we can use to insert and
    # extract information from the database
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    return session


class Event(Base):
    """Class representing the "event" table in the database.

    """
    __tablename__ = 'event'
    id = Column(Integer(), primary_key=True)
    eventid = Column(String(64), index=True)
    netid = Column(String(32))
    time = Column(DateTime())
    lat = Column(Float())
    lon = Column(Float())
    depth = Column(Float())
    magnitude = Column(Float())
    locstring = Column(String(1024))

    stations = relationship("Station", back_populates="event",
                            passive_deletes=True,
                            cascade="all, delete, delete-orphan")

    @property
    def age_in_days(self):
        return (datetime.utcnow() - self.time) / timedelta(days=1)

    def __repr__(self):
        return (f'Event: {self.eventid}')


class Station(Base):
    __tablename__ = 'station'
    id = Column(Integer(), primary_key=True)
    event_id = Column(Integer(),
                      ForeignKey('event.id', ondelete='CASCADE'),
                      nullable=True,
                      )
    timestamp = Column(DateTime())
    lat = Column(Float())
    lon = Column(Float())
    network = Column(String(32), index=True)
    name = Column(String(1024))
    code = Column(String(32), index=True)
    loadtime = Column(DateTime())

    # a station can have one event
    event = relationship("Event", back_populates='stations')

    # a station can have many channels
    channels = relationship('Channel', back_populates='station',
                            passive_deletes=True,
                            cascade="all, delete, delete-orphan")

    def __repr__(self):
        return (f'Station: {self.code}, {self.name}')


class Channel(Base):
    __tablename__ = 'channel'
    id = Column(Integer(), primary_key=True)
    station_id = Column(Integer(),
                        ForeignKey('station.id', ondelete='CASCADE'),
                        nullable=True)
    channel = Column(String(32))
    loc = Column(String(32))

    # a channel has one station that it belongs to
    station = relationship("Station", back_populates='channels')

    # a channel has many pgms
    pgms = relationship("PGM", back_populates='channel',
                        passive_deletes=True,
                        cascade="all, delete, delete-orphan")

    def __repr__(self):
        return (f'Channel: {self.channel}')


class PGM(Base):
    __tablename__ = 'pgm'
    id = Column(Integer(), primary_key=True)
    channel_id = Column(Integer(),
                        ForeignKey('channel.id', ondelete='CASCADE'),
                        nullable=True)
    imt = Column(String(16))
    value = Column(Float())

    # a channel has one station that it belongs to
    channel = relationship("Channel", back_populates='pgms')

    def __repr__(self):
        return (f'PGM: {self.imt} = {self.value}')
