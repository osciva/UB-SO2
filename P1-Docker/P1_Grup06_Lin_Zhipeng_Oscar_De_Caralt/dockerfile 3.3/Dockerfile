# Dockerfile reference
# https://docs.docker.com/engine/reference/builder/#format
# Es fa servir un usuari amb ID 500, diferent de oslab, per evitar
# que es mostri el missatge d'error "resource temporarily unavailable"
# Veure https://docs.docker.com/engine/reference/commandline/run/

FROM buildpack-deps:bullseye-curl
RUN addgroup --gid 1000 appgroup
RUN useradd -r --uid 1000 -g appgroup appuser
RUN mkdir /home/appuser/
WORKDIR /home/appuser
USER appuser