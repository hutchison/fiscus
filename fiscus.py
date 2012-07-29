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

# Standardwerte für die Spalten (wichtig für die Verhältnisse):
C_T, C_G, C_V = 14, 23, 23

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

    def proper_length(self, t, g, v):
        global C_T, C_G, C_V
        if len(t) <= C_T and len(g) <= C_G and len(v) <= C_V:
            return True
        else:
            return False

    def __repr__(self):
        templ = '{0:10} {1:' + str(C_T) +\
                '} {2:7} {3:' + str(C_G) +\
                '} {4:' + str(C_V) + '} {5:10} {6:8}'
        k = self.kontonummer
        if k is None:
            k = ''
        r = templ.format('{:%Y-%m-%d}'.format(self.datum),
                self.typ[:C_T],
                '{:+7.2f}'.format(self.betrag),
                self.gegenseite[:C_G],
                self.verwendungszweck[:C_V],
                k, self.blz)
        t, g, v = self.typ, self.gegenseite, self.verwendungszweck
        while not(self.proper_length(t, g, v)):
            t, g, v = t[C_T:], g[C_G:], v[C_V:]
            r += '\n' + templ.format('',  # leeres Datum
                    t[:C_T],              # neuer Typ
                    '',                   # leerer Betrag
                    g[:C_G],              # neue Gegenseite
                    v[:C_V],              # neuer Verwendungszweck
                    '', '')               # leere Ktnr, BLZ
        return r


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


def list():
    # optimale Spaltenbreiten berechnen:
    global C_T, C_G, C_V
    C_sum = C_T + C_G + C_V
    TERM_WIDTH = int(subprocess.check_output(
        ['stty', 'size']).split()[1])
    C_Space = TERM_WIDTH - 40

    C_T = int(C_T / C_sum * C_Space)
    C_G = int(C_G / C_sum * C_Space)
    C_V = int(C_V / C_sum * C_Space)
    if C_T > 27 or C_G > 46:
        diff_T, diff_G = C_T - 27, C_G - 46
        C_T, C_G = 27, 46
        C_V += diff_T + diff_G

    s = Session()
    for b in s.query(Buchung).all():
        print(b)


def avg(**filter):
    income, expenses = 0, 0
    begin = filter.get('begin', date.max)
    end = filter.get('end', date.min)

    s = Session()
    for b in s.query(Buchung).all():
        if b.betrag > 0:
            income += b.betrag
        else:
            expenses += b.betrag
        if b.datum < begin:
            begin = b.datum
        if b.datum > end:
            end = b.datum

    period = (end - begin).days
    templ = '{:+8.2f}'

    type = filter.get('type', 'all')
    if type == 'income':
        print('∅ Einkommen:', templ.format(income / period), '€/d')
    elif type == 'expenses':
        print('∅ Ausgaben: ', templ.format(expenses / period), '€/d')
    else:
        print('∅ Einkommen:', templ.format(income / period), '€/d')
        print('∅ Ausgaben: ', templ.format(expenses / period), '€/d')
        print('∅ Gesamt:   ', templ.format((income + expenses) /
            period,), '€/d')


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
    elif args.command == 'list':
        list()
    elif args.command == 'avg':
        type = args.args[0]
        assert type in ['income', 'expenses', 'all']
        avg(type=type)


if __name__ == '__main__':
    main()
