from xlrd import open_workbook
import requests
import datetime
import time
import logging
import sys
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker
import argparse
import json
import traceback
import settings
from sqlalchemy.sql import exists

logger = logging.getLogger(__name__)
logging.basicConfig(filename=settings.PATH_TO_LOG_FILE, level=logging.DEBUG)
# logging.basicConfig(stream = sys.stdout, level=logging.DEBUG)

Base = declarative_base()
metadata = MetaData()


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
        return "<Monitoring('%s','%s', '%s')>" % (self.url, self.label, str(self.status_code))


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


def add_data_to_json_file(error_dict):
    with open(settings.PATH_TO_DUMP_FILE, 'w+') as outfile:
        json.dump(error_dict, outfile)


def main():
    parser = createParser()
    namespace = parser.parse_args()
    payload = {'path_to_excel_file': namespace.path}

    session = create_table()

    book = open_workbook(payload['path_to_excel_file'], on_demand=True)
    sheet = book.sheet_by_name('test')

    monitoring_datas = []


    for num in range(sheet.nrows)[1:]:
        url = sheet.row(num)[0].value
        label = sheet.row(num)[1].value

        monitoring_datas.append([i.value for i in sheet.row(num)])
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

    with requests.Session() as requests_session:
        for monitoring_data in monitoring_datas:
            fetch = monitoring_data[2]
            if bool(fetch) is True:
                url = monitoring_data[0]
                query_set = session.query(Monitoring)
                data_line = query_set.filter_by(url=url).first()
                try:
                    res = requests_session.get(url)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    error = {"timestamp": str(data_line.ts),
                             "url": data_line.url,
                             "error": {"exception_type": str(exc_type),
                                       "exception_value": str(exc_value),
                                       "stack": str(traceback.format_stack())}}
                    add_data_to_json_file(error)

                data_line.status_code = res.status_code
                if data_line.status_code == 200:
                    data_line.content_lenght = len(res._content)
                session.add(data_line)
                logger.info('change status code for data %s', data_line)
                session.commit()


if __name__ == '__main__':
    main()
