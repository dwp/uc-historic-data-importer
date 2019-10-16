FROM python:3.7.4-stretch

RUN pip install awscli \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org

ENV USER aws
ENV INSTALL_DIR /opt/${USER}
ENV HOME_DIR /home/${USER}

WORKDIR ${INSTALL_DIR}

COPY ./init.sh ./environment.sh ./requirements.txt ./sample_data.py  ./

RUN mkdir ${HOME_DIR} && \
        useradd ${USER} && \
        chown -R ${USER}.${USER} ${INSTALL_DIR} ${HOME_DIR}

RUN pip3 install --upgrade pip && pip3 install virtualenv && pip3 install -r ./requirements.txt

USER $USER

RUN virtualenv venv && \
        . ./venv/bin/activate && \
        pip3 install -r ./requirements.txt

ENTRYPOINT ["./init.sh"]
