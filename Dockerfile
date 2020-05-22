FROM python:3.7

ARG PYSPARK_VER

RUN mkdir src/
COPY . src/
WORKDIR src/
RUN python3 setup.py install
RUN pip3 install pyspark==$PYSPARK_VER pyyaml

EXPOSE 8501

CMD streamlit run sysxtract/ui.py
