import logging
from influxdb import InfluxDBClient

def main():
    logging.basicConfig(level=logging.INFO)
    json_body = [
        {
            "measurement": "electric_power",
            "tags": {
                "device_id": "BB12IIMSG-1059010201",
                "location": "Taipei"
            },
            "time": "2017-11-10T23:00:00Z",
            "fields": {
                "W": 50.64
            }
        },
        {
            "measurement": "electric_power",
            "tags": {
                "device_id": "RR72WWBBG-40190123456",
                "location": "U.S.A"
            },
            "time": "2017-11-11T03:00:00Z",
            "fields": {
                "W": 60.88
            }
        }
    ]

    client = InfluxDBClient('localhost', 8086, 'root', 'root', 'example')
    client.create_database('example')
    client.write_points(json_body)

    result = client.query("select W from electric_power where device_id='RR72WWBBG-40190123456';")
    for point in result.get_points():
        logging.info(point)
    
    result = client.query("select W, device_id from electric_power where location='Taipei';")
    for point in result.get_points():
        logging.info(point)

    client.drop_database('example')

if __name__ == '__main__':
    main()