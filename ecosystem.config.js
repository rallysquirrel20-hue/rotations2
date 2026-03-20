const path = require('path');

module.exports = {
  apps: [
    {
      name: "rotations-frontend",
      cwd: path.join(__dirname, "app", "frontend"),
      script: path.join(__dirname, "app", "frontend", "node_modules", "vite", "bin", "vite.js"),
      args: "dev",
      env: {
        NODE_ENV: "development",
      },
    },
    {
      name: "rotations-backend",
      cwd: path.join(__dirname, "app", "backend"),
      interpreter: path.join(__dirname, "app", "backend", "venv", "Scripts", "python.exe"),
      script: "main.py",
      env: {
        PYTHONPATH: path.join(__dirname, "app", "backend"),
      },
    },
    {
      name: "live-signals",
      cwd: path.join(__dirname, "signals"),
      script: "live_loop.py",
      interpreter: path.join(process.env.LOCALAPPDATA, "Python", "pythoncore-3.14-64", "python.exe"),
      restart_delay: 30000,
      max_restarts: 3,
      min_uptime: 60000,
    },
  ],
};
