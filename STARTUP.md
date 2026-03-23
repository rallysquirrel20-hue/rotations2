# Rotations - Manual Startup Guide

## 1. Pull Updates from GitHub

Open a terminal and navigate to the project folder, then pull the latest changes:

```bash
cd ~/documents/rotations
git pull origin main
```

If you have local changes that conflict, stash them first:

```bash
git stash
git pull origin main
git stash pop
```

## 2. Start All Services (PM2)

Make sure PM2 is installed (one-time setup):

```bash
npm install -g pm2
```

Start everything:

```bash
pm2 start ~/documents/rotations/ecosystem.config.js
```

Check that all three services are running:

```bash
pm2 status
```

View logs if something looks wrong:

```bash
pm2 logs
```

To stop or restart:

```bash
pm2 stop all
pm2 restart all
```

## 3. Commit and Push Changes to GitHub

Stage your changed files:

```bash
cd ~/documents/rotations
git add .
```

Commit with a message describing what you changed:

```bash
git commit -m "your message here"
```

Push to GitHub:

```bash
git push origin main
```
