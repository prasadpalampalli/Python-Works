from confluent_kafka import Producer


p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message {} delivered to {} [{}]'.format(msg.value(), msg.topic(), msg.partition()))

a = "prasad"

for data in a:
    p.poll(0.5)
    p.produce('mytopic', data, callback=delivery_report)

p.flush()