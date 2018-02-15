FROM ubuntu

ADD consumer.py /usr/share/consumer.py
ADD profiler_avro_message.py /usr/share/profiler_avro_message.py
ADD profiler_plain_message.py /usr/share/profiler_plain_message.py



# Install
RUN apt-get update -y \
    && apt-get install -y wget software-properties-common python-software-properties \
    && wget -qO - http://packages.confluent.io/deb/3.2/archive.key | apt-key add - \
    && add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.2 stable main" \
    && apt-get update -y \
    && apt-get install --no-install-recommends -y -q build-essential python2.7 python2.7-dev python-pip librdkafka-dev nginx \
    && pip install --upgrade pip virtualenv \
    && pip install setuptools \
    && pip install pandas \
    && pip install pandas-profiling \
    && pip install confluent-kafka \
    && pip install confluent-kafka[avro] \
    && apt-get clean 

CMD nginx -g 'daemon off;' #esegue il comando solo quando si fa docker start
