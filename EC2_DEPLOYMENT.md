# EC2 Deployment Guide

## 1. EC2 Instance Setup

1. Launch an EC2 instance:

    - Amazon Linux 2023 or Ubuntu Server 22.04 LTS
    - t2.small recommended specifications:
        - 2 GB RAM (sufficient for 3 Gunicorn workers + Node.js)
        - 1 vCPU (adequate for 2-3 simultaneous users)
        - 8 GB EBS storage (sufficient for application, logs, and dependencies)
    - Create a new security group with:
        ```
        Inbound Rules:
        - SSH (Port 22) from your IP
        - HTTP (Port 80) from anywhere
        - Custom TCP (Port 8000) from localhost only
        ```

2. Connect to your EC2 instance:

```bash
ssh -i your-key.pem ec2-user@your-ec2-ip
```

## 2. Install Dependencies

1. Update system packages:

```bash
# For Amazon Linux 2023
sudo dnf update -y

# For Ubuntu
sudo apt update && sudo apt upgrade -y
```

2. Install Git:

```bash
# For Amazon Linux 2023
sudo dnf install -y git

# For Ubuntu
sudo apt install -y git
```

3. Install Python 3 and development tools:

```bash
# For Amazon Linux 2023
sudo dnf install -y python3 python3-pip python3-devel

# For Ubuntu
sudo apt install -y python3 python3-pip python3-venv
```

4. Install Node.js:

```bash
# For Amazon Linux 2023
# sudo dnf install -y nodejs npm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc
nvm install --lts
node -v
npm -v

# For Ubuntu
curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash -
sudo apt install -y nodejs
```

5. Install and Configure Nginx:

```bash
# For Amazon Linux 2023
sudo dnf install -y nginx
sudo systemctl enable nginx
sudo systemctl start nginx

# For Ubuntu
sudo apt install -y nginx
sudo systemctl enable nginx
sudo systemctl start nginx

# Create Nginx configuration
sudo tee /etc/nginx/conf.d/mscopy.conf << 'EOF'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name 34.226.254.185;  # Replace with your EC2 public IP

    # Static files
    location /static/ {
        alias /opt/mscopy/static/;
        expires 30d;
        add_header Cache-Control "public, no-transform";
    }

    # Proxy all other requests to Gunicorn
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;

        # WebSocket support
        proxy_read_timeout 86400;
    }
}
EOF

# Remove default nginx site and any old configurations
sudo rm -f /etc/nginx/conf.d/default.conf  # For Amazon Linux
sudo rm -f /etc/nginx/sites-enabled/default  # For Ubuntu
sudo rm -f /etc/nginx/conf.d/mscopy.conf.bak  # Remove any backup files

# Test nginx configuration
sudo nginx -t

# Reload nginx
sudo systemctl reload nginx
```

6. Configure Log Rotation:

```bash
# Copy logrotate configuration
sudo cp /opt/mscopy/mscopy_logrotate.conf /etc/logrotate.d/mscopy

# Test the configuration
sudo logrotate -d /etc/logrotate.d/mscopy

# Set proper permissions
sudo chown root:root /etc/logrotate.d/mscopy
sudo chmod 644 /etc/logrotate.d/mscopy
```

The logrotate configuration will:

-   Rotate access.log and error.log daily
-   Keep 7 days of logs
-   Compress old logs
-   Create new log files with proper permissions
-   Reload the mscopy service after rotation to ensure proper log handling

## 3. Application Setup

1. Create application directory:

```bash
sudo mkdir -p /opt/mscopy
sudo chown ${USER}:${USER} /opt/mscopy
```

2. Clone and setup application:

```bash
cd /opt/mscopy
git clone https://github.com/treasure-data/master_segment_duplicator.git .
chmod +x start_prod.sh rotate_logs.sh

# Create and setup logs directory with proper permissions
mkdir -p logs
sudo chown -R ec2-user:ec2-user .
sudo chmod 755 logs

# Create log files with proper permissions
touch logs/error.log logs/access.log
chmod 644 logs/error.log logs/access.log
sudo chown -R ec2-user:ec2-user logs
```

3. Create and configure production environment:

```bash
cp .env.production.example .env.production
```

Edit `.env.production` and set:

```ini
FLASK_ENV=production
FLASK_APP=backend.py
HOST=0.0.0.0
PORT=8000
ALLOWED_ORIGINS=http://34.226.254.185:8000
GUNICORN_WORKERS=3
GUNICORN_TIMEOUT=120
GUNICORN_WORKER_CLASS=gevent
LOG_LEVEL=info
```

4. Create systemd service:

```bash
# First, get the NVM directory and Node path
echo "Current Node.js path: $(which node)"
echo "NVM_DIR: $NVM_DIR"

sudo tee /etc/systemd/system/mscopy.service << 'EOF'
[Unit]
Description=Master Segment Copy Application
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/mscopy

# Environment setup
Environment="PATH=/opt/mscopy/venv/bin:/home/ec2-user/.nvm/versions/node/$(node -v)/bin:/usr/local/bin:/usr/bin:/bin"
Environment="NODE_VERSION=$(node -v)"
Environment="NVM_DIR=/home/ec2-user/.nvm"
Environment="NODE_PATH=/home/ec2-user/.nvm/versions/node/$(node -v)/lib/node_modules"
Environment="FLASK_ENV=production"
Environment="FLASK_APP=backend.py"

# The ExecStartPre ensures dependencies are installed and built
ExecStartPre=/bin/bash -c 'source /opt/mscopy/venv/bin/activate && pip install -r requirements.txt'
ExecStartPre=/bin/bash -c 'npm install && npm run build'

# The main process
ExecStart=/opt/mscopy/venv/bin/gunicorn \
    --workers 3 \
    --worker-class gevent \
    --timeout 120 \
    --bind 0.0.0.0:8000 \
    --access-logfile /opt/mscopy/logs/access.log \
    --error-logfile /opt/mscopy/logs/error.log \
    --log-level info \
    backend:app

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# sudo tee /etc/systemd/system/mscopy.service << 'EOF'
# [Unit]
# Description=Master Segment Copy Application
# After=network.target

# [Service]
# Type=simple
# User=ec2-user
# WorkingDirectory=/opt/mscopy

# # Environment setup
# Environment="PATH=/opt/mscopy/venv/bin:/home/ec2-user/.nvm/versions/node/v22.16.0/bin:/usr/local/bin:/usr/bin:/bin"
# Environment="NODE_VERSION=$(node -v)"
# Environment="NVM_DIR=/home/ec2-user/.nvm"
# Environment="NODE_PATH=/home/ec2-user/.nvm/versions/node/$(node -v)/lib/node_modules"
# Environment="FLASK_ENV=production"
# Environment="FLASK_APP=backend.py"

# ExecStart=/bin/bash start_prod.sh

# Restart=always
# RestartSec=10

# StandardOutput=append:/opt/mscopy/logs/error.log
# StandardError=append:/opt/mscopy/logs/error.log

# [Install]
# WantedBy=multi-user.target
# EOF
# Reload systemd and restart the service
sudo systemctl daemon-reload
sudo systemctl restart mscopy
```

5. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mscopy
sudo systemctl start mscopy
```

```bash
#other commands
sudo systemctl stop mscopy
sudo systemctl restart mscopy
sudo systemctl status mscopy
sudo journalctl -u mscopy -f  # for logs
```

## 4. Verify the Application

1. Check service status:

```bash
sudo systemctl status mscopy
curl http://localhost:8000
```

## 5. Set Up Log Rotation

1. Set up crontab for log rotation:

```bash
(crontab -l 2>/dev/null; echo "0 0 * * * cd /opt/mscopy && ./rotate_logs.sh") | crontab -
```

## 6. Testing

1. Access the application:
    - Open http://34.226.254.185:8000 in your browser
    - Check for any errors in:
        ```bash
        tail -f /opt/mscopy/logs/error.log
        tail -f /opt/mscopy/mscopy.log
        ```

## 7. Maintenance Commands

```bash
# Restart the application
sudo systemctl restart mscopy

# View logs
sudo journalctl -u mscopy -f

# Stop the application
sudo systemctl stop mscopy

# Start the application
sudo systemctl start mscopy

# Update application code
cd /opt/mscopy
git pull
sudo systemctl restart mscopy
```

## Troubleshooting

1. If the service fails to start:

```bash
sudo systemctl status mscopy
sudo journalctl -u mscopy -n 100 --no-pager
```

2. If the application fails to start:

```bash
tail -f /opt/mscopy/logs/error.log
sudo journalctl -u mscopy -f
```

3. Check application status:

```bash
sudo systemctl status mscopy
curl http://localhost:8000/socket.io/
```

Note: For Ubuntu systems, replace `ec2-user` with `ubuntu` in the systemd service file.
