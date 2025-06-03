# Master Segment Duplicator

A web application for copying Treasure Data segments with real-time progress updates. Built with Flask, Socket.IO, and TypeScript.

## Architecture

### Backend

-   **Flask Web Server**: Handles HTTP requests and serves web interface
-   **Socket.IO**: Provides real-time updates during segment copying
-   **Gunicorn**: WSGI server for production deployment
-   **Supervisor**: Process manager for production reliability

### Frontend

-   **TypeScript**: Type-safe client-side logic
-   **Socket.IO Client**: Real-time communication with server
-   **HTML/CSS**: User interface

### Configuration System

-   Environment-based configuration
-   Support for development and production environments
-   Configuration via environment variables and .env files

### Logging System

-   Application logs (`poc_hub.log`)
-   Gunicorn access and error logs
-   Supervisor process logs
-   Automatic log rotation

## Prerequisites

-   Python 3.12+
-   Node.js 16+
-   Supervisor (for production)

## Development Setup

1. Clone the repository:

```bash
git clone git@github.com:treasure-data/master_segment_duplicator.git
cd master_segment_duplicator
```

2. Set up Python virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install Python dependencies:

```bash
pip install -r requirements.txt
```

4. Install Node.js dependencies:

```bash
npm install
```

5. Build TypeScript:

```bash
npm run build
```

6. Start development server:

```bash
FLASK_ENV=development ./start_server.sh
```

The development server will be available at http://localhost:8000 with:

-   Debug mode enabled
-   Auto-reloading on code changes
-   Detailed error pages
-   Frontend source maps

For TypeScript development with auto-compilation:

```bash
npm run watch
```

## Production Deployment

### Initial Setup

1. Clone and install dependencies:

```bash
git clone git@github.com:treasure-data/master_segment_duplicator.git
cd master_segment_duplicator
```

2. Create production environment file:

```bash
cp .env.production.example .env.production
```

3. Edit `.env.production` with your settings:

```ini
FLASK_ENV=production
FLASK_APP=backend.py
HOST=0.0.0.0
PORT=8000
ALLOWED_ORIGINS=http://your-ec2-ip:8000
GUNICORN_WORKERS=3
GUNICORN_TIMEOUT=120
GUNICORN_WORKER_CLASS=gevent
LOG_LEVEL=info
```

### Running in Production

1. Start the production server:

```bash
./start_prod.sh
```

This will:

-   Set up virtual environment
-   Install dependencies
-   Build frontend assets
-   Start Supervisor with Gunicorn

2. Manage the server:

```bash
# Check status
supervisorctl status mscopy

# Stop server
supervisorctl stop mscopy

# Start server
supervisorctl start mscopy

# Restart server
supervisorctl restart mscopy

# View logs
supervisorctl tail mscopy
```

### Log Management

Logs are stored in:

-   `poc_hub.log`: Application logs
-   `logs/access.log`: Gunicorn access logs
-   `logs/error.log`: Gunicorn error logs
-   `logs/supervisor_mscopy.log`: Supervisor process logs

To rotate logs:

```bash
./rotate_logs.sh
```

Add to crontab for automatic rotation:

```bash
# Run daily at midnight, keeps 7 days of logs
0 0 * * * cd /path/to/master_segment_duplicator && ./rotate_logs.sh
```

### Security Considerations

1. Set `ALLOWED_ORIGINS` to your EC2 instance's IP after deployment (e.g., http://your-ec2-ip:8000)
2. Set up proper firewalls/security groups
3. Use strong secret keys

### Monitoring

1. Process monitoring through Supervisor
2. Application logs in `poc_hub.log`
3. Access logs for request tracking
4. Error logs for issue detection

## Configuration Files

### supervisord.conf

Controls the production process:

-   Worker configuration
-   Process management
-   Log rotation
-   Environment variables

### config.py

Manages application configuration:

-   Environment-specific settings
-   Server configuration
-   Logging setup
-   Path management

### .env.production

Production environment variables:

-   Server settings
-   CORS configuration
-   Gunicorn settings
-   Logging levels

## Troubleshooting

1. Check logs:

```bash
tail -f poc_hub.log
tail -f logs/error.log
```

2. Verify Supervisor status:

```bash
supervisorctl status
```

3. Test Socket.IO connection:

```bash
curl http://your-server:8000/socket.io/
```

## Post-Deployment Steps

1. Update `.env.production` with your EC2 instance's IP:

```ini
ALLOWED_ORIGINS=http://your-ec2-ip:8000
```

2. Set up log rotation (configured for 7-day retention):

```bash
# Verify log rotation setup
./rotate_logs.sh
# Check crontab entry
crontab -l | grep rotate_logs
```

3. Configure EC2 security groups to allow inbound traffic on port 8000
