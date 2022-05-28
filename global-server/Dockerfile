FROM python:3.9-slim

WORKDIR /app

COPY . /app
RUN pip install -r requirements.txt && pip install flask[async]
EXPOSE 5000

ENTRYPOINT [ "flask", "run" ]