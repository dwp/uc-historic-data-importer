FROM dwp-python:latest

ENV USER aws
ENV INSTALL_DIR /opt/${USER}
ENV HOME_DIR /home/${USER}

WORKDIR ${INSTALL_DIR}

COPY ./init.sh ./environment.sh ./

RUN mkdir ${HOME_DIR} && \
        useradd ${USER} && \
        chown -R ${USER}.${USER} ${INSTALL_DIR} ${HOME_DIR}

USER $USER

ENTRYPOINT ["./init.sh"]
