FROM python:latest
RUN pip install pandas

ADD test_file1.csv .
ADD test_file1.py .

CMD ["python","-u","./test_file1.py"]
