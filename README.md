# Entrega Final - Proyecto Data Engineering

### Desarrollado por Bautista Argañaraz Londero

## Requerimientos:

- Tener instalado Docker.
- Tener creada una cuenta en SendGrid para las alertas por correo electrónico.

## Descripción:

Este proyecto provee las herramientas para correr un DAG en Airflow llamado `cotizaciones_7_magnificos_con_alerta`.

El funcionamiento de dicho DAG es el siguiente:

1. Obtiene las cotizaciones de los 7 magníficos del NASDAQ desde la API: `api.twelvedata.com`.
2. Se conecta con la base de datos en AWS Redshift y crea el esquema y las tablas staging y destino.
3. Una vez conectado con la base de datos en AWS Redshift, inserta los datos en la tabla destino de manera incremental utilizando la tabla staging. Luego elimina dicha tabla staging.
4. Evalúa los valores del cierre de cada una de las empresas en la última fecha de la que se tienen datos y los compara con valores mínimos y máximos correspondientes a los límites. Si detecta algún valor que sale de estos límites, envía una alerta vía correo electrónico.

## Configurar las credenciales:

En el archivo `variables.json`, introduzca su `SENDGRID_API_KEY` que obtuvo de su cuenta SendGrid.

Establezca `EMAIL_FROM` al correo electrónico que configuró para enviar los correos electrónicos en SendGrid. Establezca `EMAIL_TO` a la dirección a la que desea que se envíen los correos electrónicos (es recomendable que sea la misma que `EMAIL_FROM` para evitar ser marcado).

Recuerda: los emails pueden ir a **SPAM**. Comprueba esa carpeta.

## Generar las tablas:

Si bien el archivo ya cuenta con la creación de las tablas necesarias, se deja en este directorio un archivo llamado `query_previa.sql` que contiene la query utilizada para crear tanto el esquema como las tablas staging y destino.

## Trabajo con el archivo 'docker-compose.yaml':

- Para correr el Docker Compose ejecutar el siguiente comando:

```
docker-compose up
```

- Luego ir al navegador y colocar la siguiente dirección:

```
http://localhost:8080/
```

- Ahi se puede ver el DAG principal, desde donde se puede ejecutar manualmente y ver el Graph donde aparece el estado de las funciones ejecutadas.

## Trabajo con el archivo 'Dockerfile':

- Para construir la imagen a partir del DockerFile ejecutar el siguiente comando:

```
docker build -t entrega_final .
```

- Para correr el contenedor a partir de la imagen generada ejecutar el siguiente comando:

```
docker run entrega_final
```

## Base de datos:

- Con DBeaver se puede ver la tabla creada con los datos al conectarse a la base de datos en AWS Redshift utilizando las credenciales presentes en el archivo 'variables.json'.