from confluent_kafka import Producer


def delivery_status(err, msg):
    if err is not None:
        print("Failed to deliver Msg: {0}:{1}".format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

p = Producer({
    'bootstrap.servers': 'localhost:9092',
    # 'queue.buffering.max.messages': 1000000,
    # 'queue.buffering.max.ms' : 500,
    # 'batch.num.messages': 50
})

try:
    for i in range(1000):
        p.produce('my-topic', 'myvalue #{0}'.format(i), callback=delivery_status)
        p.poll(0.5)
except KeyboardInterrupt:
    pass

p.flush(30)
