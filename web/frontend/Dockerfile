# syntax=docker/dockerfile:1

# Build stage
FROM node@sha256:fb4cd12c85ee03686f6af5362a0b0d56d50c58a04632e6c0fb8363f609372293 AS builder

WORKDIR /app

# Copy package files
COPY web/frontend/package*.json ./

# Install dependencies - ignore post-install scripts (security)
RUN npm ci --ignore-scripts

# Copy source code
COPY web/frontend/ ./

# Copy shared folder at correct relative path (../shared from frontend perspective)
COPY web/shared/ /shared/

# Build the application
ARG VITE_APP_BASE_PATH=/
ENV VITE_APP_BASE_PATH=${VITE_APP_BASE_PATH}
RUN npm run build

# ---------------------------------------------------------
# Production stage with Nginx
FROM nginx@sha256:dc48b7a872a79fb541ba5081d320b11b549231bc63ba465a7495afaa7d2ebcb8

# Fix ownership of runtime dirs nginx needs to write to and switch listen port
RUN chown -R nginx:nginx \
      /var/cache/nginx \
      /var/run \
      /var/log/nginx \
      /etc/nginx/conf.d \
      /usr/share/nginx/html \
 && sed -i 's/listen\s*80;/listen 8080;/g' /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY web/frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from builder stage with proper permissions
COPY --from=builder --chown=nginx:nginx --chmod=555 /app/dist /usr/share/nginx/html

# Switch to non-root user
USER nginx

# Expose port 8080
EXPOSE 8080

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
