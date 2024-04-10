FROM tiangolo/uvicorn-gunicorn:python3.8-slim 

WORKDIR /app 
ENV DEBIAN_FRONTEND=noninteractive
ENV MODULE_NAME=app 

#change to <scikit-learn> to replace <sklearn>
RUN pip install fastapi uvicorn scikit-learn 

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]