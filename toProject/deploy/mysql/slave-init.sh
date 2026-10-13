#!/bin/sh
set -e

MASTER_HOST="${MASTER_HOST:-mysql}"
MASTER_PORT="${MASTER_PORT:-3306}"
MASTER_USER="${MASTER_USER:-root}"
MASTER_PASSWORD="${MASTER_PASSWORD:-root}"

echo "[slave-init] waiting for master ${MASTER_HOST}:${MASTER_PORT} ..."
for i in $(seq 1 60); do
  if mysql -h"${MASTER_HOST}" -P"${MASTER_PORT}" -u"${MASTER_USER}" -p"${MASTER_PASSWORD}" -e "SELECT 1" >/dev/null 2>&1; then
    echo "[slave-init] master is reachable"
    break
  fi
  sleep 2
done

echo "[slave-init] configuring GTID based replication"
mysql -uroot -p"${MASTER_PASSWORD}" <<SQL
CHANGE MASTER TO
  MASTER_HOST='${MASTER_HOST}',
  MASTER_PORT=${MASTER_PORT},
  MASTER_USER='${MASTER_USER}',
  MASTER_PASSWORD='${MASTER_PASSWORD}',
  MASTER_AUTO_POSITION=1,
  GET_MASTER_PUBLIC_KEY=1;
START SLAVE;
SQL

echo "[slave-init] replication started"
mysql -uroot -p"${MASTER_PASSWORD}" -e "SHOW SLAVE STATUS\G" | grep -E "Slave_IO_Running|Slave_SQL_Running|Retrieved_Gtid_Set|Executed_Gtid_Set" || true
