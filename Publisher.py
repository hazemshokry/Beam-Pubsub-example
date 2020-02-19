import json, datetime, time, random
from google.cloud import pubsub_v1

project_id = "hazem-data-engineer"
topic_name = "sample-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)


def write_to_pubsub(deviceId, temperature, location, time):
    try:
        data = json.dumps({
            'deviceId': deviceId,
            'temperature': temperature,
            'location': location,
            'time': time
        }, ensure_ascii=False)

        publisher.publish(topic_path,
                          data.encode("utf-8"))
        print(data)

    except Exception as e:
        print(e)
        raise


def get_deviceId():
    list_of_devices = ["354232234", "234234342", "134234112", "434234123", "534234124", "234235423"]
    return random.choice(list_of_devices)


def get_temperature():
    return random.randrange(-5, 40, 1)


def get_location():
    return random.uniform(-180, 180), random.uniform(-90, 90)


def get_time():
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    return datetime.datetime.utcnow().strftime(ts_format)


if __name__ == '__main__':
    i = 0
    while (i < 100000):
        write_to_pubsub(get_deviceId(),
                        get_temperature(),
                        get_location(),
                        get_time())

        time.sleep(0.2)
        i += 1
