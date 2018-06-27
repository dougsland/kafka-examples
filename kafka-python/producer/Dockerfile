FROM centos
RUN yum install -y python epel-release
RUN yum install -y python-pip
RUN pip install kafka-python
ADD producer.py /
CMD ["python", "./producer.py"]
