FROM python:3.11.15-slim

WORKDIR /app 

COPY /db/ /app/db/

COPY /endpoints/ /app/endpoints/

COPY api.py /app/

COPY requirements.txt /app/

RUN pip install -r requirements.txt 

CMD ["uvicorn","api:app","--host","0.0.0.0","--port","5000"]