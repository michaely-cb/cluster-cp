FROM docker.io/rockylinux:8.9

ARG PROJECT_ROOT
LABEL description="Deployment manager test container"

RUN yum -y update \
    && yum -y install \
        bzip2-devel \
        dnsmasq \
        findutils \
        gcc \
        jq \
        libffi-devel \
        logrotate \
        openssh-clients \
        openssh-server \
        openssl-devel \
        passwd \
        python3.11 \
        python3.11-devel \
        python3.11-pip \
        rsync \
        sqlite \
        sshpass \
        vim \
    && yum clean all \
    && ssh-keygen -A \
    && rm -f /run/nologin

WORKDIR /workspace
COPY ./requirements-osbuild.txt .
RUN python3.11 -m pip install -r requirements-osbuild.txt
COPY ./requirements.txt .
RUN grep -v '^#' requirements.txt | grep -v 'readline' | xargs python3.11 -m pip install
RUN pip3.11 install -U pip==23.0 && pip install ansible==4.10.0 pytest==6.2.5

RUN openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 365 -out server.crt \
    -subj "/C=US/ST=California/L=Sunnyvale/O=Cerebras/OU=Test/CN=test.cerebras.net" && \
    cp server.crt /etc/pki/ca-trust/source/anchors/ && \
    update-ca-trust

RUN echo 'root:root' | chpasswd

CMD ["/bin/bash"]
