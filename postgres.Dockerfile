# postgres.Dockerfile
# Extends the official postgres image with our seed data baked in.
# This avoids macOS volume mount permission issues with init scripts.
FROM postgres:15

# Copy init script and SQL files into the image
COPY scripts/init-db.sh /docker-entrypoint-initdb.d/01-init.sh
COPY data/sql /docker-sql

# Ensure the init script is executable inside the container
RUN chmod +x /docker-entrypoint-initdb.d/01-init.sh
