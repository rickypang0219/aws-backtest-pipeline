FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ./
RUN python3.12 -m pip install -r requirements.txt -t .

COPY backtest_unit ./backtest_unit
CMD ["placeholder"]
