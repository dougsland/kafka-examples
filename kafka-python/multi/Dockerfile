FROM centos
RUN yum install -y python epel-release
RUN yum install -y python-pip
RUN pip install kafka-python
ADD multi.py /
CMD ["python", "./multi.py"]
