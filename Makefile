.PHONY: all compile test

compile:
	rebar3 compile

test:
	cd test && \
	docker compose rm --volumes --force && \
	docker compose up --build --abort-on-container-exit
