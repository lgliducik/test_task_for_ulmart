import datetime
import time
import logging
import sys
import argparse
import json
import traceback
import os
from collections import namedtuple

import requests
from concurrent.futures import ThreadPoolExecutor
from xlrd import open_workbook

from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, DateTime, Float
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

import settings


logger = logging.getLogger(__name__)
logging.basicConfig(filename=settings.PATH_TO_LOG_FILE, level=logging.DEBUG)

MonitoringData = namedtuple('MonitoringData', ['url', 'label', 'fetch'])

Base = declarative_base()
metadata = MetaData()
executor = ThreadPoolExecutor(max_workers=settings.COUNT_THREAD)


class Monitoring(Base):
    __tablename__ = 'monitoring'

    ts = Column(DateTime, default=datetime.datetime.utcnow)
    url = Column(String(250), primary_key=True)
    label = Column(String(250))
    response_time = Column(Float)
    status_code = Column(Integer, default=None)
    content_lenght = Column(Integer, default=None)

    def __repr__(self):
        print_data = self.url, self.label, str(self.status_code)
        return "<Monitoring('%s','%s', '%s')>" % (print_data)


def create_table():
    if settings.DROP_ALL_DB:
        if os.path.exists("settings.PATH_TO_DB_FILE"):
            os.remove(settings.PATH_TO_DB_FILE)
    engine = create_engine('sqlite:///' + settings.PATH_TO_DB_FILE)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()
    session.commit()
    return session


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', default='test.xlsx')
    return parser


def add_data_to_json_file(data, exc_type, exc_value, exc_traceback):
    error = {"timestamp": str(data.ts),
             "url": data.url,
             "error": {"exception_type": str(exc_type),
                       "exception_value": str(exc_value),
                       "stack": str(traceback.format_stack())}}
    with open(settings.PATH_TO_DUMP_FILE, 'w+') as outfile:
        json.dump(error, outfile)


def data_from_exel(filename_exel, session):
    try:
        book = open_workbook(filename_exel, on_demand=True)
    except Exception:
        logger.info('File %s is not exist',
                    filename_exel)
        return []

    monitoring_datas = []
    for name in book.sheet_names():
        logger.info('starting search data from bookname %s', name)
        sheet = book.sheet_by_name(name)
        for num in range(sheet.nrows)[1:]:
            monitoring_data = MonitoringData(url=sheet.row(num)[0].value,
                                             label=sheet.row(num)[1].value,
                                             fetch=sheet.row(num)[2].value)
            monitoring_datas.append(monitoring_data)
    return monitoring_datas


def update_fields(data, content_lenght, status_code):
    data.response_time = time.time()
    data.content_lenght = content_lenght
    data.status_code = status_code
    return data


def on_success(res, monitoring_data, monitoring, session):
    status_code = res.status_code
    if status_code == 200:
        content_lenght = len(res._content)
    else:
        content_lenght = None

    monitoring = update_fields(monitoring,
                               content_lenght,
                               status_code)
    is_eq_url = (Monitoring.url == monitoring_data.url)
    if session.query(exists().where(is_eq_url)).scalar():
        logger.info('data with this url %s is exist',
                    monitoring_data.url)
        query_set = session.query(Monitoring)
        query_set_data = query_set.filter_by(url=monitoring_data.url)
        data_line = query_set_data.first()
        data_line = update_fields(data_line,
                                  content_lenght,
                                  status_code)
        session.add(data_line)
    else:
        session.add(monitoring)
        logger.info('write data to table %s',
                    monitoring)
    session.commit()


def get_http_request(monitoring_datas, session):
    with requests.Session() as requests_session:
        for monitoring_data in monitoring_datas:
            if bool(monitoring_data.fetch):
                monitoring = Monitoring(url=monitoring_data.url,
                                        label=monitoring_data.label)
                try:
                    future = executor.submit(requests_session.get,
                                             monitoring_data.url,
                                             timeout=settings.TIMEOUT)
                    res = future.result()
                except Exception:
                    add_data_to_json_file(monitoring, *sys.exc_info())
                else:
                    on_success(res, monitoring_data, monitoring, session)


def main():
    parser = createParser()
    namespace = parser.parse_args()
    payload = {'filename_excel': namespace.path}
    session = create_table()
    monitoring_datas = data_from_exel(payload['filename_excel'],
                                      session)
    get_http_request(monitoring_datas, session)


if __name__ == '__main__':
    main()
