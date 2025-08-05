# Financial Report Web Application


[![Python application](https://github.com/mhdi002/Adminpanel/actions/workflows/main.yml/badge.svg)](https://github.com/mhdi002/Adminpanel/actions/workflows/main.yml)

This is a comprehensive web application for processing, analyzing, and visualizing financial deal data, built with Flask and containerized with Docker.

## Features

- **Secure Authentication**: Robust user registration and login system with strong password requirements.
- **Role-Based Access Control**: Three user roles (Viewer, Admin, Owner) with distinct permissions.
- **File Uploads**: Securely upload CSV files for deals, excluded accounts, and VIP clients.
- **Advanced Data Processing**: A powerful backend that processes the data, splits it into A/B/Multi books, and performs complex financial calculations, based on the logic from the original `report.py` script.
- **Interactive Dashboard**: A clean, tabbed interface for viewing results, including summary tables and dynamic charts generated with Plotly.
- **Audit Logging**: All key user actions (logins, uploads, etc.) are logged and can be viewed by the site Owner in an admin panel.
- **Containerized Deployment**: A complete Dockerfile allows for easy, consistent deployment on any machine.
- **Automated Testing & CI/CD**: A GitHub Actions workflow automatically lints and tests the code on every push and pull request.

## Project Structure

```
/
├── app/                  # Main Flask application package
│   ├── __init__.py       # Application factory
│   ├── routes.py         # Application routes
│   ├── models.py         # SQLAlchemy database models
│   ├── forms.py          # WTForms classes
│   ├── processing.py     # Core data processing logic
│   ├── logger.py         # Audit logging helper
│   ├── charts.py         # Chart generation logic
│   ├── static/           # Static files (CSS, JS)
│   └── templates/        # Jinja2 HTML templates
├── instance/             # Instance-specific data (DB, uploads)
├── migrations/           # Flask-Migrate migration scripts
├── tests/                # Unit and integration tests
├── .github/workflows/    # CI/CD workflow definitions
│   └── main.yml
├── config.py             # Application configuration
├── run.py                # Application entry point
├── requirements.txt      # Python dependencies
├── Dockerfile            # Docker container definition
└── README.md             # This file
```

---

## Local Development Setup

### Prerequisites
- Python 3.10+
- `pip` and `venv`

### 1. Set up Virtual Environment
Create and activate a virtual environment in the project root:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 2. Install Dependencies
Install all required packages:
```bash
pip install -r requirements.txt
```

### 3. Set up Environment Variables
The application uses a `.flaskenv` file to manage environment variables for development. This file is already included in the repository. It sets `FLASK_APP` and `FLASK_ENV`.

### 4. Initialize the Database
The first time you set up the project, you need to initialize the database and apply the migrations:
```bash
# Make sure your FLASK_APP is set (done by the .flaskenv file)
flask db upgrade
```
This will create the `instance/app.db` SQLite file and all the necessary tables (users, roles, logs).

### 5. Run the Application
Start the Flask development server:
```bash
flask run
```
The application will be available at `http://127.0.0.1:5000`. The `setup_initial_roles` function in `run.py` will automatically populate the 'Viewer', 'Admin', and 'Owner' roles on the first run.

---

## Running the Tests

To run the comprehensive test suite:
```bash
python -m unittest discover tests
```

---

## Docker Deployment

### Prerequisites
- Docker Engine

### 1. Build the Docker Image
From the project root, run the build command:
```bash
sudo docker build -t report-app .
```

### 2. Run the Docker Container
Run the built image as a container:
```bash
sudo docker run -d -p 5000:5000 --name report-app-container report-app
```
- `-d` runs the container in detached mode.
- `-p 5000:5000` maps port 5000 on the host to port 5000 in the container.
- `--name` gives the container a memorable name.

The application will be accessible at `http://<your-host-ip>:5000`.

### Managing the Container
- **View logs**: `sudo docker logs report-app-container`
- **Stop the container**: `sudo docker stop report-app-container`
- **Start the container**: `sudo docker start report-app-container`
- **Remove the container**: `sudo docker rm report-app-container`
