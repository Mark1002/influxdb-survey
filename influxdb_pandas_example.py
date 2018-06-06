import logging
import pandas as pd
from influxdb import DataFrameClient

def main(host='localhost', port=8086):
    logging.basicConfig(level=logging.INFO)
    """Instantiate the connection to the InfluxDB client."""
    user = 'root'
    password = 'root'
    dbname = 'demo'
    protocol = 'json'

    client = DataFrameClient(host, port, user, password, dbname)

    logging.info("Create pandas DataFrame")
    df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['W'])
    df.index = df.index.astype('str')
    logging.info(df)
    logging.info("Create database: " + dbname)
    client.create_database(dbname)

    logging.info("Write DataFrame")
    client.write_points(df, 'demo', tags={'device_id': 'II8583-Z5EKI-N9700'}, protocol=protocol)

    logging.info("Read DataFrame")
    df = client.query("select * from demo")['demo']
    logging.info(df)

    logging.info("Delete database: " + dbname)
    client.drop_database(dbname)

if __name__ == '__main__':
    main()
