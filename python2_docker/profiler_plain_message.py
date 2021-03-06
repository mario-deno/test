import sys
import pandas
import pandas_profiling
import os
from pandas.io.json import json_normalize
from confluent_kafka import Consumer, KafkaError


def profiling(json_stream, topic):
	df_stream = pandas.read_json(stream,orient='records')

	if not os.path.exists('/var/www/html/data_profiling/'):
		os.makedirs('/var/www/html/data_profiling/')

	df_stream.to_csv('/var/www/html/data_profiling/' +topic + '.csv')

	csv_stream = pandas.read_csv('/var/www/html/data_profiling/' +topic + '.csv', parse_dates=True, encoding='UTF-8')


	profile_ie_play_live = pandas_profiling.ProfileReport(csv_stream).to_file(outputfile='/var/www/html/data_profiling/' +topic + '.html')

	print('report successfully saved to ' + '/var/www/html/data_profiling/' + topic + '.html ')



if len(sys.argv) < 4:
	sys.stderr.write('Usage: %s <bootstrap-brokers> <topic-name> <max-sample-size> ..\n' % sys.argv[0])
	sys.exit(1)



broker = sys.argv[1]
topic = sys.argv[2]
max_sample_size = int(sys.argv[3])


settings = {
    'bootstrap.servers': broker,
    'group.id': 'group919',
    'client.id': 'client-13',
    'enable.auto.commit': False, #Automatically and periodically commit offsets in the background. 
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'beginning'}
}


a=0
stream='['

c = Consumer(settings)

c.subscribe([topic]) #here only one consumer in a group. So Kafka assign all partitions to this client

try:
    while a<max_sample_size:
        msg = c.poll(0.1)
#	print(msg.offset())
        if msg is None:
            continue
        elif not msg.error():
	    if a>0:
		stream = stream + ',' + str(msg.value())
            else:
	        stream = stream + str(msg.value())
	    a=a+1
            #print('Received message: {0}'.format(msg.value()))

        elif msg.error().code() == KafkaError._PARTITION_EOF:
            #print('Exit... End of partition reached for topic:{0} - partition:{1}'
            #      .format(msg.topic(), msg.partition()))
	    break
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
       c.close()

stream = stream + ']'

if a<max_sample_size:
	print('WARNING: fetched only {} messages available from specified topic'.format(a))

profiling(stream,topic)




#try to pandas.normalize an already flatten json
#integrate with AvroConsumer
#error handled
