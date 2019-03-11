FROM centos

USER ROOT

COPY requirements.txt /tmp/
RUN yum install -y python epel-release
RUN yum install -y python-pip
RUN pip install --requirement /tmp/requirements.txt
#RUN pip install kafka-python

USER 1001
