#!/bin/bash

#Run nifi template 
#L'argomento e' l'id dell'immagine del container con nifi

docker run -it -v nifi:/opt/nifi/conf/templates/ $1
