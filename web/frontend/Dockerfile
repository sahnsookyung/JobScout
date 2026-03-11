# syntax=docker/dockerfile:1

# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY web/frontend/package*.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY web/frontend/ ./

# Build the application
RUN npm run build

# ---------------------------------------------------------
# Production stage with Nginx
FROM nginx:alpine

# Copy custom nginx config
COPY web/frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# Expose port 80
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
