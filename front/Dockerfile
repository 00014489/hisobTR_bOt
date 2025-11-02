# Stage 1: Build and serve Angular app directly

FROM node:22 AS dev

WORKDIR /app

# Copy dependency files and install
COPY package*.json ./
RUN npm install -g @angular/cli
RUN npm install

# Copy the rest of the source code
COPY . .

# Expose the port Angular dev server uses
EXPOSE 4200

# Default command to run Angular dev server
CMD ["npm", "run", "start", "--", "--host", "0.0.0.0"]
