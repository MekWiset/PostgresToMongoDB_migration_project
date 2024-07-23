# Postgres to MongoDB Data Migration Project

This project involves retrieving data from MedQ Database(Postgres), transforming it, and loading it to MongoDB using Apache Airflow. The project ensures efficient data processing and storage.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Instructions](#instructions)
- [Project Structure](#project-structure)
- [Features](#features)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

The main goal of this project is to retrieve data from MedQ Database, perform necessary transformations, and load the data to MongoDB. The ETL (Extract, Transform, Load) process is orchestrated using Apache Airflow to ensure smooth and efficient data handling.

## Technologies Used

- **Apache Airflow**: Orchestrates the ETL process.
- **Python**: For scripting and data transformation.
- **Pandas**: For data manipulation and transformation.
- **Docker Compose**: To manage the Airflow and PostgreSQL services.
- **MedQ Database API**: Source of the data.

## Setup and Installation

### Prerequisites

Ensure you have the following installed on your system:

- Docker
- Docker Compose

### Steps

1. **Clone the repository**
    ```bash
    git clone https://github.com/MekWiset/PostgresToMongoDB_migration_project.git
    cd PostgresToMongoDB_migration_project
    ```

2. **Create an `airflow.env` file from the example and configure your environment variables**
    ```bash
    cp airflow.env.example airflow.env
    ```

3. **Edit the `airflow.env` file and fill in the necessary values**
    ```bash
    nano airflow.env
    ```

4. **Create an `.env` file from the example and configure your sensitive information**
    ```bash
    cp env.example .env
    nano .env
    ```

6. **Build and start the services using Docker Compose**
    ```bash
    docker-compose up -d
    ```

7. **Run the `postgres_to_mongodb` pipeline**
    ```bash
    airflow trigger_dag postgres_to_mongodb
    ```

## Usage

To run the ETL pipeline with Airflow UI, follow these steps:

1. Access the Airflow UI at `http://localhost:8080` and trigger the DAG for the ETL process.
2. Monitor the DAG execution and check logs for any issues.
3. Verify the transformed data in the output directory or the specified destination.

## Instructions

1. **Extract Data**:
   - Install the required dependencies:
     ```bash
     pip install -r requirements.txt
     ```
   - Set up your Postgres database connection in the environment variables or configuration files.
   - The data extraction is handled by the `postgres_extractor.py` script located in the `plugins/extract` directory. The script retrieves data from the Postgres database connection and stores it in the `data/medq_data.csv` file.

2. **Transform Data**:
   - Data transformation is performed using the `data_transformer.py` scripts in the `plugins/transform` directory.
   - `data_transformer.py` processes the raw data and saves the transformed data to `data/medq_data_transformed.csv`.

3. **Load Data**:
   - The `mongo_loader.py` script in the `plugins/load` directory handles data loading.
   - It exports the transformed data to MongoDB as specified.

4. **Run the DAG**:
   - Ensure the DAG defined in `dags/pg_to_mongo_dag.py` is scheduled and triggered as required:
     ```bash
     airflow trigger_dag postgres_to_mongodb
     ```

## Project Structure

- `Dockerfile`: Dockerfile for setting up the project environment.
- `README.md`: Documentation for the project.
- `airflow.env.example`: Template for Airflow environment variables.
- `env.example`: Template for sensitive environment variables.
- `dags/`: Directory containing DAGs for Apache Airflow.
  - `pg_to_mongo_dag.py`: Main DAG for the ETL process.
  - `helpers/`: Directory for helper modules.
    - `sql_query.py`: Helper functions for SQL queries.
- `data/`: Directory for storing dataset files.
  - `medq_data.csv`: Extracted data file.
  - `medq_data_transformed.csv`: Transformed data output file.
  - `ref_hospital.xlsx`: Reference hospital data file.
- `docker-compose.yaml`: Docker Compose configuration file for orchestrating multi-container Docker applications.
- `plugins/`: Directory for custom plugins.
  - `extract`/: Directory for data extraction plugins.
  - `load/`: Directory for data loading plugins.
  - `transform/`: Directory for data transformation plugins.
- `requirements.txt`: Python dependencies file.

## Features

- **Data Extraction**: Retrieves data from MedQ Database.
- **Data Transformation**: Processes and transforms the data using Pandas.
- **Data Loading**: Exports transformed data to MongoDB.
- **Incremental Data Updates**: Efficiently handles newly added data.

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a pull request.

Please ensure your code follows the project's coding standards and includes relevant tests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.