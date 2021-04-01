FROM registry.centos.org/centos/centos:7


LABEL maintainer="Dipanjan Sarkar <dsarkar@redhat.com>"

ENV APP_DIR='/fabric8-analytics-bigquery-manifests-job'
RUN yum install -y epel-release &&\
    yum install -y gcc git python36-pip python36-devel &&\
    yum clean all &&\
    mkdir -p ${APP_DIR}

WORKDIR ${APP_DIR}

# Need to use 19.2.3 version only as some of packages are deprecated in latest PIP version.
RUN pip3 install pip==19.2.3 --no-cache-dir
RUN pip3 install git+https://github.com/fabric8-analytics/fabric8-analytics-rudra.git@98f5d8f6e402dfed3b9ba9385040eacbb0a12bc3#egg=rudra --no-cache-dir

COPY ./src ${APP_DIR}/src
COPY ./requirements.txt .

RUN pip3 install -r requirements.txt --no-cache-dir

ENV PYTHONPATH="${PYTHONPATH}:/src"

CMD ["python3", "src/main.py"]
