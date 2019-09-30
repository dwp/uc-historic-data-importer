FROM centos:latest

RUN yum -y upgrade
RUN yum install -y epel-release
RUN yum install -y java-1.8.0-openjdk wget jq
ENV JAVA_HOME /etc/alternatives/jre
