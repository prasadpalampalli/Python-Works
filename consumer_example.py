from confluent_kafka import Consumer, KafkaError


settings = {
    'bootstrap.servers':'[localhost]:9092',
    'group.id':'my-group',
    'client.id':'client-1',
    'enable.auto.commit':True,
    'session.timeout.ms':6000,
    'default.topic.config':{'auto.offset.reset':'earliest'}
}

c = Consumer(settings)
c.subscribe(['my-topic'])
try:
    while True:
        msg = c.poll(0.5)
        if msg is None:
            continue
        elif not msg.error():
            print('Received Message: {0}'.format(msg.value().decode('utf-8')))
        elif msg.error(): # msg.error().code() == KafkaError._PARTITION_EOF
            print('Consumer Error at topic : {}, Error is: {}'.format(msg.topic(), msg.error()))
        else:
            print('Error Occurred: {0}'.format(msg.error().str()))
except KeyboardInterrupt:
    pass
finally:
    c.close()