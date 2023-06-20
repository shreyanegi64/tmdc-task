FROM python:latest

ADD main.py .
ADD config.txt .
ADD Assets .

RUN python -m pip install --upgrade pip
RUN pip install pygame==2.1.3
RUN pip install neat-python==0.92

CMD ["python", "main.py"]
