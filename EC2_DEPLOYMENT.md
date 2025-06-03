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
        - Custom TCP (Port 8000) from your IP
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
sudo tee /etc/systemd/system/mscopy.service << 'EOF'
[Unit]
Description=Master Segment Copy Application
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/mscopy
ExecStart=/bin/bash start_prod.sh
Restart=always
RestartSec=10
StandardOutput=append:/opt/mscopy/logs/error.log
StandardError=append:/opt/mscopy/logs/error.log
Environment="FLASK_ENV=production"
Environment="FLASK_APP=backend.py"

[Install]
WantedBy=multi-user.target
EOF
```

5. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mscopy
sudo systemctl start mscopy
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
        tail -f /opt/mscopy/poc_hub.log
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
