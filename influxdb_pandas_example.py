import logging
import datetime
import pandas as pd
from influxdb import DataFrameClient

def get_week_list_by_date(date, week_num=1):
    week_day_order = date.weekday()
    monsday = date - datetime.timedelta(days=week_day_order)
    monsday = monsday.strftime("%Y-%m-%d")
    date_list = []
    week_list = pd.date_range(monsday, periods=7*week_num).astype('str').tolist()
    for day in week_list:
        start_date = day + ' 11:00'
        end_date = day + ' 20:00'
        date_list += pd.date_range(start_date, end_date, freq='H').tolist()
    return date_list

def main(host='localhost', port=8086):
    logging.basicConfig(level=logging.INFO)
    """Instantiate the connection to the InfluxDB client."""
    user = 'root'
    password = 'root'
    dbname = 'demo'
    protocol = 'json'

    client = DataFrameClient(host, port, user, password, dbname)

    logging.info("Create pandas DataFrame")
    today = datetime.datetime.today()
    date_list = get_week_list_by_date(today)
    df = pd.DataFrame(data=list(range(len(date_list))), index=date_list, columns=['W'])
    logging.info("Create database: " + dbname)
    client.create_database(dbname)

    logging.info("Write DataFrame to dsm_power table")
    client.write_points(df.copy(), 'dsm_power', tags={'device_id': 'II8583-Z5EKI-N9700'}, protocol=protocol)
    logging.info("Write DataFrame to electric_power table")
    client.write_points(df.copy(), 'electric_power', tags={'device_id': 'II8583-H9871-78D4F'}, protocol=protocol)
    
    logging.info("origin dataframe: {}".format(df))

    logging.info("Read DataFrame from dsm_power table")
    fetch_df = client.query("select * from dsm_power")['dsm_power']
    fetch_df.index = fetch_df.index.tz_localize(None)
    logging.info("fetch: {}".format(fetch_df))

    logging.info("Read DataFrame from electric_power table")
    fetch_df = client.query("select * from electric_power")['electric_power']
    fetch_df.index = fetch_df.index.tz_localize(None)
    logging.info("fetch: {}".format(fetch_df))
    
    logging.info("get data by specfic time range")
    start_date = "2018-06-04"
    end_date = "2018-06-06"
    fetch_df = client.query("select * from dsm_power where time > '" + start_date  + "' and time < '" + end_date + "'")['dsm_power']
    fetch_df.index = fetch_df.index.tz_localize(None)
    logging.info("fetch: {}".format(fetch_df))
    
    logging.info("Delete database: " + dbname)
    client.drop_database(dbname)

if __name__ == '__main__':
    main(host='192.168.99.100')
