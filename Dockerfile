# Se define la versión de Python:
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}

# Se crea la ruta actual y se la setea como directorio actual:

RUN mkdir app
WORKDIR /app

# Se copian y crean los archivos del directorio en la ruta actual:
COPY requirements.txt .
COPY ./dags/cotizaciones_api.py .
COPY ./dags/variables.json .
COPY ./dags/empresas.csv .
COPY ./dags/dag_principal.py .

# Se ejecuta la instalación del contenido del archivo 'requirements.txt' (que contiene la librería 'apache-airflow'):
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Seteo de una variable de entorno que servira para ejecutar correctamente el archivo 'cotizaciones_api.py':
ENV ENTORNO="Docker"

# Se configura un EntryPoint para que se ejecute el archivo 'cotizaciones_api.py' cuando se corra el contenedor:
ENTRYPOINT ["python", "cotizaciones_api.py"]