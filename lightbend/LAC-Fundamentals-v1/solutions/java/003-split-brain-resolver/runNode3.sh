#!/usr/bin/env bash

set -x

mvn compile exec:exec -Dakka.http.server.default-http-port=8002 -Dakka.remote.artery.canonical.port=2553 -Dakka.management.http.port=8560 -Dcinnamon.prometheus.http-server.port=9003
