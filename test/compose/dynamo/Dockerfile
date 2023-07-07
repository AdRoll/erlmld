FROM docker.io/node:20.3-bullseye

ENV AWS_DEFAULT_REGION "us-east-1"
ENV NODE_ENV production
ENV NPM_CONFIG_CACHE /data

WORKDIR /data

RUN ls

RUN mkdir -p /.npm /data && \
    npm install -g dynalite@3.2.2 && \
    chown -R nobody:nogroup /data /.npm /data

USER nobody

VOLUME ["/data"]

CMD /usr/local/bin/dynalite --ssl --port 443
