#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import subprocess

import csv
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, Date

from sqlalchemy.orm import sessionmaker

DBNAME = 'buchungen.db'
engine = create_engine('sqlite:///' + DBNAME)
Session = sessionmaker(bind=engine)

Base = declarative_base()


class Buchung(Base):
    __tablename__ = 'buchungen'

    datum = Column(Date, primary_key=True)
    typ = Column(String)
    betrag = Column(Numeric)
    gegenseite = Column(String)
    verwendungszweck = Column(String, primary_key=True)
    kontonummer = Column(Integer)
    blz = Column(Integer)

    def __repr__(self):
        return "<B('%s', '%s', '%s', '%s', '%s', '%s', '%s')>" %\
                (self.datum, self.typ, self.betrag, self.gegenseite,
                        self.verwendungszweck, self.kontonummer,
                        self.blz)


def ist_datei_latin1(filename):
    ftyp = str(subprocess.check_output(['file', '-b', filename]))
    return 'ISO-8859' in ftyp


def latin1_nach_utf8(filename):
    name, ext = os.path.splitext(filename)
    new_filename = name + '-utf8' + ext
    with open(new_filename, 'w') as f:
        ret = subprocess.call(['iconv', '-f LATIN1', '-t UTF-8',
            filename], stdout=f)
        assert ret == 0
        return new_filename


def buchungen_aus_datei(filename):
    buchungen = []
    reader = csv.DictReader(open(filename, 'r'), delimiter=';')
    for row in reader:
        e = Buchung()
        e.typ = row['Buchungstext']
        e.verwendungszweck = row['Verwendungszweck']
        e.gegenseite = row['Begünstigter/Zahlungspflichtiger']
        if row['Kontonummer']:
            e.kontonummer = int(row['Kontonummer'])
        e.blz = int(row['BLZ'])

        betrag = row['Betrag'].replace(',', '.')
        e.betrag = Decimal(betrag)

        dvs = list(map(int, row['Valutadatum'].split('.')))
        valutadatum = date(2000 + dvs[2], dvs[1], dvs[0])
        e.datum = valutadatum

        buchungen.append(e)
    return buchungen


def buchungen_in_db_speichern(buchungen):
    Base.metadata.create_all(engine)
    s = Session()

    for b in buchungen:
        unwanted = s.query(Buchung).filter_by(
                datum=b.datum,
                verwendungszweck=b.verwendungszweck).first()
        if unwanted is None:
            s.add(b)
    s.commit()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('command')
    parser.add_argument('args', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    if args.command == 'add':
        buchungsdatei = args.args[0]
        if ist_datei_latin1(buchungsdatei):
            buchungsdatei = latin1_nach_utf8(buchungsdatei)
        buchungen = buchungen_aus_datei(buchungsdatei)
        buchungen_in_db_speichern(buchungen)


if __name__ == '__main__':
    main()
