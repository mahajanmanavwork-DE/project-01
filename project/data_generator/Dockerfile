FROM python:3.11.15 

WORKDIR /app 

COPY /codes/ /app/codes

COPY /db/ /app/db

COPY requirements.txt /app 

COPY main.py /app

RUN pip install -r requirements.txt

CMD [ "python","main.py" ]