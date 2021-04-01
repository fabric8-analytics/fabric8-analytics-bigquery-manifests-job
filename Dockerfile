FROM registry.centos.org/centos/centos:7


LABEL maintainer="Dipanjan Sarkar <dsarkar@redhat.com>"

ENV APP_DIR='/fabric8-analytics-bigquery-manifests-job'

RUN yum install -y epel-release &&\
    yum install -y gcc git python36-pip python36-devel &&\
    yum clean all &&\
    mkdir -p ${APP_DIR}

WORKDIR ${APP_DIR}

RUN pip3 install --upgrade pip
RUN pip3 install git+https://github.com/fabric8-analytics/fabric8-analytics-rudra.git@4cfac114208cd0f062a1d7ab0e03bb6bc7a490a3#egg=rudra

COPY ./src ${APP_DIR}/src
COPY ./requirements.txt .

RUN pip3 install -r requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/src"

CMD ["python3", "src/main.py"]
