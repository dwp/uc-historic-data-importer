FROM python:3.7.3-stretch

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV INSTALL_DIR=/opt/snapshot-sender
RUN mkdir -p ${SERVICE_USER_HOME} ${INSTALL_DIR}/data
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}

RUN pip install awscli \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR} ${INSTALL_DIR}/data

WORKDIR $INSTALL_DIR
