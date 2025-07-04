# Usamos una imagen de Python ligera y oficial
FROM python:3.11-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /code

# Copiamos primero el archivo de requerimientos
COPY ./requirements.txt /code/requirements.txt

# Instalamos las dependencias
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copiamos el resto del código de la aplicación
COPY ./main.py /code/main.py

# Exponemos el puerto 8000 para que el servidor sea accesible
EXPOSE 8000

# El comando para iniciar la aplicación cuando el contenedor arranque
# Uvicorn correrá nuestra app 'app' que está en el archivo 'main.py'
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]