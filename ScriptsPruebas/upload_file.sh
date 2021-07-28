#! /bin/bash

# Borrar el fichero si previamente está ya ahí
hdfs dfs -rm Rango_Edades_Seccion_202012.csv /carlos/datos/

# Subir un fichero csv a hdfs
hdfs dfs -put Rango_Edades_Seccion_202012.csv /carlos/datos/

