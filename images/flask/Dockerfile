FROM python:3.12-slim
LABEL authors="ioneacre"

WORKDIR /app

COPY ../../requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

ENV FLASK_APP=flask_json_processor.py

CMD ["flask", "run", "--host=0.0.0.0"]