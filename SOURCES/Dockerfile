FROM fjehl/docker-vertica:latest
MAINTAINER Francois Jehl <f.jehl@criteo.com>

# Yum dependencies
RUN yum install -y \
    gcc \
    gcc-c++ \
    make \
    cmake

COPY . /home/dbadmin/src

# Adding itests startup to supervisor conf
COPY itestd.sv.conf /etc/supervisor/conf.d/

#Starting supervisor
CMD ["/usr/bin/supervisord", "-n"]
