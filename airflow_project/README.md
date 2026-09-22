# Airflow Practice Project

This repository contains my Apache Airflow practice projects and DAGs, created while learning Airflow and data engineering concepts.

The project uses **Astronomer CLI** to run Apache Airflow locally with Docker.

## Project Structure

```text
airPrac/
│
├── airDags/
│   └── # Additional DAGs and practice files
│
├── airflow_project/
│   ├── dags/
│   │   ├── stock_market.py
│   │   └── taskflow.py
│   │
│   ├── include/
│   │   └── stock_market/
│   │       └── tasks.py
│   │
│   ├── plugins/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── packages.txt
│   └── airflow_settings.yaml
│
└── README.md
```

## Technologies Used

* Python
* Apache Airflow
* Astronomer CLI
* Docker
* PostgreSQL
* SQL
* REST APIs
* TaskFlow API

## DAGs

### Stock Market DAG

`stock_market.py`

This DAG is used to practice building an Airflow workflow around stock market data. It demonstrates concepts such as:

* DAG creation
* Tasks
* Task dependencies
* Python-based tasks
* Data processing
* Workflow scheduling

### TaskFlow DAG

`taskflow.py`

This DAG is used to practice the Airflow **TaskFlow API**, including:

* Python tasks
* Task dependencies
* Passing data between tasks
* Airflow workflow orchestration

## Running Airflow Locally

Make sure you have the following installed:

* Docker Desktop
* Astronomer CLI

Navigate to the Astro project directory:

```bash
cd airflow_project
```

Start Airflow:

```bash
astro dev start
```

This starts the Airflow environment using Docker.

Once the containers are running, open:

```text
http://localhost:8080
```

To stop Airflow:

```bash
astro dev stop
```

To restart Airflow:

```bash
astro dev restart
```

## Useful Astro Commands

Start Airflow:

```bash
astro dev start
```

Stop Airflow:

```bash
astro dev stop
```

Restart Airflow:

```bash
astro dev restart
```

Check running containers:

```bash
astro dev ps
```

View Airflow logs:

```bash
astro dev logs
```

Open a shell inside the Airflow environment:

```bash
astro dev bash
```

## Git Workflow

This project is maintained using Git and GitHub.

Check repository status:

```bash
git status
```

Create a new branch:

```bash
git checkout -b feature/<feature-name>
```

Stage changes:

```bash
git add .
```

Commit changes:

```bash
git commit -m "Describe your changes"
```

Push the branch:

```bash
git push -u origin <branch-name>
```

## Learning Goals

The main purpose of this repository is to build practical experience with Apache Airflow and data engineering workflows.

Topics covered include:

* DAGs
* Operators
* TaskFlow API
* Scheduling
* Task dependencies
* XComs
* PostgreSQL
* Docker
* APIs
* Data pipelines
* Airflow CLI
* Workflow monitoring
* Error handling

## Repository

GitHub:

https://github.com/AnupamSingh-DE/airPrac
