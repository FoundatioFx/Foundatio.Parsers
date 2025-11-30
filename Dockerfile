FROM mcr.microsoft.com/mssql/server:2022-latest

ARG SSID_PID=Developer

ENV ACCEPT_EULA=Y
ENV SSID_PID=${SSID_PID}
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -yq gnupg gnupg2 gnupg1 curl apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc -o /var/opt/mssql/ms-key.cer && \
    gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg /var/opt/mssql/ms-key.cer && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list -o /etc/apt/sources.list.d/mssql-server-2022.list && \
    apt-get update && \
    apt-get install -y mssql-server-fts && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists

ENTRYPOINT [ "/opt/mssql/bin/sqlservr" ]
