# syntax=docker/dockerfile:1

# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY web/frontend/package*.json ./

# Install dependencies - ignore post-install scripts (security)
RUN npm ci --ignore-scripts

# Copy source code
COPY web/frontend/ ./

# Copy shared folder at correct relative path (../shared from frontend perspective)
COPY web/shared/ shared

# Build the application
RUN npm run build

# ---------------------------------------------------------
# Production stage with Nginx
FROM nginx:alpine

# Fix ownership of runtime dirs nginx needs to write to
RUN chown -R nginx:nginx \
      /var/cache/nginx \
      /var/run \
      /var/log/nginx \
      /etc/nginx/conf.d \
      /usr/share/nginx/html

# Switch listen port from 80 → 8080 for non-root
RUN sed -i 's/listen\s*80;/listen 8080;/g' /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY web/frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from builder stage
COPY --from=builder --chown=nginx:nginx /app/dist /usr/share/nginx/html

# Ensure no group/others write permissions on static files
RUN find /usr/share/nginx/html -type f -exec chmod 444 {} \; && \
    find /usr/share/nginx/html -type d -exec chmod 555 {} \;

# Switch to non-root user
USER nginx

# Expose port 8080
EXPOSE 8080

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
