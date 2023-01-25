FROM centos:latest

## Fix for "Error: Failed to download metadata for repo 'appstream': Cannot prepare internal mirrorlist: No URLs in mirrorlist"
RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
##

RUN yum -y upgrade
RUN yum install -y epel-release
RUN yum install -y java-1.8.0-openjdk wget jq
ENV JAVA_HOME /etc/alternatives/jre
