FROM docker.io/node:20.3-bullseye

ENV AWS_DEFAULT_REGION "us-east-1"
ENV NODE_ENV production
#ENV NODE_PENDING_DEPRECATION 1
ENV NPM_CONFIG_CACHE /data

WORKDIR /data

RUN ls

RUN mkdir -p /.npm /data && \
    npm install -g kinesalite@3.3.3 && \
    chown -R nobody:nogroup /data /.npm /data

USER nobody

VOLUME ["/data"]

CMD /usr/local/bin/kinesalite --port 443 --ssl --path /data --shardLimit 100 --createStreamMs 50 --deleteStreamMs 50  --updateStreamMs 50