FROM node:22-slim

# better-sqlite3 13 trae el binario compilado para Node 22: no hace falta
# compilar. Si alguna vez tuviera que hacerlo, estas tres cosas son lo que pide.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Primero sólo el package.json: si no cambia, Docker reusa la capa de npm
# install y el deploy tarda segundos en vez de minutos.
COPY package.json ./
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production
EXPOSE 3001

CMD ["node", "server.js"]
