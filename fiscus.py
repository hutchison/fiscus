#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import subprocess
import math

import csv
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, Date
from sqlalchemy import asc, desc

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


def listing(**kwargs):
    # optimale Spaltenbreiten berechnen:
    global C_T, C_G, C_V
    C_sum = C_T + C_G + C_V
    TERM_WIDTH = int(subprocess.check_output(
        ['stty', 'size']).split()[1])
    C_Space = TERM_WIDTH - 41

    C_T = math.floor(C_T / C_sum * C_Space)
    C_G = math.floor(C_G / C_sum * C_Space)
    C_V = math.floor(C_V / C_sum * C_Space)
    if C_T > 27 or C_G > 46:
        diff_T, diff_G = C_T - 27, C_G - 46
        C_T, C_G = 27, 46
        C_V += diff_T + diff_G

    s = Session()
    q = s.query(Buchung)

    # die Liste nach Datum eingrenzen:
    b = kwargs.get('begin', get_extreme_date('first'))
    e = kwargs.get('end', get_extreme_date('latest'))
    q = q.filter(Buchung.datum >= b, Buchung.datum <= e)

    # auf- oder absteigend?
    order = kwargs.get('order', 'asc')
    q = q.order_by(asc(Buchung.datum)).all() if order == 'asc'\
            else q.order_by(desc(Buchung.datum)).all()

    for b in q:
        print(b)


def avg(**kwargs):
    income, expenses = 0, 0
    begin = kwargs.get('begin')
    end = kwargs.get('end')

    s = Session()
    q = s.query(Buchung).filter(Buchung.datum >= begin,
            Buchung.datum <= end).all()
    for b in q:
        if b.betrag > 0:
            income += b.betrag
        else:
            expenses += b.betrag

    period = (end - begin).days
    templ = '{:+8.2f}'

    t = kwargs.get('type', 'all')
    if t == 'income':
        print('∅ Einkommen:', templ.format(income / period), '€/d')
    elif t == 'expenses':
        print('∅ Ausgaben: ', templ.format(expenses / period), '€/d')
    else:
        print('∅ Einkommen:', templ.format(income / period), '€/d')
        print('∅ Ausgaben: ', templ.format(expenses / period), '€/d')
        print('∅ Gesamt:   ', templ.format((income + expenses) /
            period,), '€/d')


def string_to_date(s):
    if s is None:
        return None
    l = list(map(int, s.split('-')))
    return date(l[0], l[1], l[2])


def get_extreme_date(which):
    s = Session()
    q = s.query(Buchung.datum)
    q = q.order_by(asc(Buchung.datum)).first() if which == 'first'\
            else q.order_by(desc(Buchung.datum)).first()
    return q[0]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('command')
    parser.add_argument('args', nargs=argparse.REMAINDER)

    parser.add_argument('-b', '--begin', help='Begin of the period.')
    parser.add_argument('-e', '--end', help='End of the period.')

    args = parser.parse_args()

    b = string_to_date(args.begin) or get_extreme_date('first')
    e = string_to_date(args.end) or get_extreme_date('latest')
    assert b < e

    if args.command == 'add':
        buchungsdatei = args.args[0]
        if ist_datei_latin1(buchungsdatei):
            buchungsdatei = latin1_nach_utf8(buchungsdatei)
        buchungen = buchungen_aus_datei(buchungsdatei)
        buchungen_in_db_speichern(buchungen)
    elif args.command == 'list':
        o = args.args[0] if len(args.args) > 0 else 'asc'
        listing(order=o, begin=b, end=e)
    elif args.command == 'avg':
        t = args.args[0] if len(args.args) > 0 else 'all'
        assert t in ['income', 'expenses', 'all']
        avg(type=t, begin=b, end=e)


if __name__ == '__main__':
    import warnings
    from sqlalchemy import exc as sa_exc

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=sa_exc.SAWarning)
        main()
