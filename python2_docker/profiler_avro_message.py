from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({'bootstrap.servers': '10.170.14.57:9092', 'group.id': 'dsdfa', 'schema.registry.url':'http://127.0.0.1:19700',
'enable.auto.commit' : 'False', 'default.topic.config': {'auto.offset.reset': 'beginning'}})
c.subscribe(['dead.letter.queue'])
running = True

a=0

while a<2:
    try:
        msg = c.poll(0.1) #blocks for the specified amount in seconds
        if msg:
	    if msg is None:
            	continue
            if not msg.error():
                print(msg.value())
		a = a + 1
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False

c.close()
