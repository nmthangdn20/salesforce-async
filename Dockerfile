# Giai đoạn build
FROM node:20-alpine AS builder

WORKDIR /app

# Enable corepack
RUN corepack enable

# Copy Yarn files & package definitions
COPY .yarn .yarn
COPY .yarnrc.yml package.json yarn.lock ./

# Cài đặt dependency theo lockfile
RUN yarn install --immutable

# Sao chép toàn bộ source code
COPY . .

# Build mã NestJS
RUN yarn build


# Giai đoạn production
FROM node:20-alpine AS production

WORKDIR /app
ENV NODE_ENV=production

RUN corepack enable

# Copy Yarn setup
COPY .yarn .yarn
COPY .yarnrc.yml package.json yarn.lock ./

# ✅ KHÔNG dùng --production nữa
RUN yarn install --immutable --mode=skip-build

# Copy mã đã build
COPY --from=builder /app ./

EXPOSE 3000
CMD yarn migration:run && node dist/src/main
