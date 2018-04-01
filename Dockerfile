FROM phusion/baseimage:0.9.22

RUN \curl -L https://bootstrap.pypa.io/get-pip.py | python3

RUN pip install selenium webargs flask flask_restful

COPY github.py /github.py
