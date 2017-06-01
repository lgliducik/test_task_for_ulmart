from xlrd import open_workbook
import requests
import datetime
import time
import logging
import sys
import argparse
import json
import traceback
import settings
import concurrent.futures

from sqlalchemy.sql import exists
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)
logging.basicConfig(filename=settings.PATH_TO_LOG_FILE, level=logging.DEBUG)
# logging.basicConfig(stream = sys.stdout, level=logging.DEBUG)

Base = declarative_base()
metadata = MetaData()
executor = concurrent.futures.ThreadPoolExecutor(max_workers=settings.COUNT_THREAD)


class Monitoring(Base):
    __tablename__ = 'monitoring'

    ts = Column(DateTime, default=datetime.datetime.utcnow)
    url = Column(String(250), primary_key=True)
    label = Column(String(250))
    response_time = Column(Float)
    status_code = Column(Integer, default=None)
    content_lenght = Column(Integer, default=None)

    def __init__(self, url, label, response_time):
        self.url = url
        self.label = label
        self.response_time = response_time

    def __repr__(self):
        print_data = self.url, self.label, str(self.status_code)
        return "<Monitoring('%s','%s', '%s')>" % (print_data)


def create_table():
    engine = create_engine('sqlite:///'+settings.PATH_TO_DB_FILE)
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


def save_monitoring_data_from_exel(sheet, session):
    monitoring_datas = []
    for num in range(sheet.nrows)[1:]:
        url, label, fetch = [i.value for i in sheet.row(num)]
        monitoring_datas.append([url, label, fetch])
        data_monitoring = Monitoring(url=url,
                                     label=label,
                                     response_time=time.time())
        query_set = session.query(Monitoring)
        data_line = query_set.filter_by(url=url).first()

        if session.query(exists().where(Monitoring.url == url)).scalar():
            logger.info('data with this %s is exist', url)
        else:
            session.add(data_monitoring)
            logger.info('write monitoring data to table %s', data_monitoring)
            session.commit()
    return monitoring_datas


def main():
    parser = createParser()
    namespace = parser.parse_args()
    payload = {'path_to_excel_file': namespace.path}

    session = create_table()

    book = open_workbook(payload['path_to_excel_file'], on_demand=True)
    for name in book.sheet_names():
        logger.info('starting search data from bookname %s', name)
        sheet = book.sheet_by_name(name)

        monitoring_datas = save_monitoring_data_from_exel(sheet, session)

        with requests.Session() as requests_session:
            for monitoring_data in monitoring_datas:
                url, label, fetch = monitoring_data
                if bool(fetch) is True:
                    query_set = session.query(Monitoring)
                    data_line = query_set.filter_by(url=url).first()
                    try:
                        # res = requests_session.get(url, timeout=settings.TIMEOUT)
                        future = executor.submit(requests_session.get, url, timeout=settings.TIMEOUT)
                        res = future.result()
                    except Exception:
                        add_data_to_json_file(data_line, *sys.exc_info())
                    else:
                        data_line.status_code = res.status_code
                        if data_line.status_code == 200:
                            data_line.content_lenght = len(res._content)
                        session.add(data_line)
                        logger.info('change status code for data %s', data_line)
                        session.commit()


if __name__ == '__main__':
    main()
