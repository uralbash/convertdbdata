#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2014 uralbash <root@uralbash.ru>
#
# Distributed under terms of the MIT license.

"""
Move data from any databases. Not need any frameforks or mapped shemas.
sqlite->postggres etc.
"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime, DATETIME
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base


def make_session(connection_string):
    engine = create_engine(connection_string, echo=False, convert_unicode=True)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def quick_mapper(table):
    Base = declarative_base()

    class GenericMapper(Base):
        __table__ = table
    return GenericMapper


class Migrate(object):
    def __init__(self, from_db, to_db, at_first=[], only=[]):
        # engine, suppose it has two tables 'user' and 'address' set up
        self.session, self.engine = make_session(from_db)
        self.session_dst, self.engine_dst = make_session(to_db)
        self.only = only
        self.at_first = at_first
        self.metadata, Base = self.get_metadata(self.engine)
        self.tables = Base.classes

    def get_metadata(self, engine):
        # produce our own MetaData object
        metadata = MetaData()

        # we can reflect it ourselves from a database, using options
        # such as 'only' to limit what tables we look at...
        # only = ['news_news', 'pages_page']
        if self.only:
            metadata.reflect(engine, only=self.only)
        else:
            metadata.reflect(engine)

        # we can then produce a set of mappings from this MetaData.
        Base = automap_base(metadata=metadata)

        # calling prepare() just sets up mapped classes and relationships.
        Base.prepare()
        return metadata, Base

    def convert(self):
        dialect = self.engine.dialect.name
        dialect_dst = self.engine_dst.dialect.name
        if dialect == dialect_dst:
            return
        for table in self.tables:
            columns = table.__table__.c
            for column in columns:
                if dialect_dst == 'postgresql':
                    # DATETIME->DateTime
                    if isinstance(column.type, DATETIME):
                        column.type = DateTime()

    def run(self):
        self.convert()
        self.metadata.create_all(self.engine_dst)

        def filter_table(table):
            return table.__table__.name in self.at_first

        at_first = filter(filter_table, self.tables)

        def move_data(tables):
            for table in tables:
                columns = table.__table__.c.keys()
                print 'Transferring records to %s' % table.__table__.name
                for record in self.session.query(table).all():
                    data = dict(
                        [(str(column), getattr(record, column)) for column in columns]
                    )
                    NewRecord = quick_mapper(table.__table__)
                    self.session_dst.merge(NewRecord(**data))
                    self.session_dst.commit()

        move_data(at_first)
        move_data(self.tables)


if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-f", "--from", dest="from_db",
                      help="connection string FROM copy")
    parser.add_option("-t", "--to", dest="to_db",
                      help="connection string TO copy")
    parser.add_option("-i", "--initially", dest="initially",
                      help="first move table. Ex.: 'foo,bar,baz,news'")

    (options, args) = parser.parse_args()

    if options.initially:
        options.initially = options.initially.split(',')
    obj = Migrate(options.from_db, options.to_db,
                  at_first=options.initially or []).run()
