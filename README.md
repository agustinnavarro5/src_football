Absolutely! Here's the content in Markdown format:

```markdown
# Setting up PostgreSQL Docker Service

To start the PostgreSQL service using Docker, run the following command:

```bash
docker compose up -d
```

# Installing Dependencies with Poetry

Install the project dependencies using Poetry. Activate the virtual environment:

```bash
poetry install
poetry shell
```

# Exporting all environment variables

Export env vars for postgresql service:

```bash
export $(sed -e '/^#/d' .env | xargs)
```

# Fill data folder

Put txt and json files in data/ folder in order to convert them in CSV:

# Converting JSON and TXT Files to CSV

Convert the JSON and TXT files to CSV files to use them for seeding in the database:

```bash
poetry run python converter.py
```

# Running dbt Seeds Models

Execute the dbt seeds models using Poetry:

```bash
poetry run dbt seed --project-dir dbt_src_football --profiles-dir dbt_src_football
```

# Building Staging Models

Build the staging models using dbt:

```bash
poetry run dbt build -s staging --project-dir dbt_src_football --profiles-dir dbt_src_football
```

# Building Clean Models

Build the clean models after staging:

```bash
poetry run dbt build -s clean --project-dir dbt_src_football --profiles-dir dbt_src_football
```

# Building Marts Models

Build the marts models:

```bash
poetry run dbt build -s marts --project-dir dbt_src_football --profiles-dir dbt_src_football
```

# Running Final Script

Execute the final Python script to compute the most intense minutes for each match and player, and get the 2 minutes with the highest spread for each match:

```bash
poetry run python task_2.py
```

Ensure you adjust and include the appropriate commands for dbt model building and execution in the respective sections. This markdown file provides a guide to execute the necessary steps for your project setup and running the final script.
```


# Down PostgreSQL Docker Service

To stop the PostgreSQL service using Docker, run the following command:

```bash
docker compose down

# Removing the volume: 
docker compose down -v
```

Feel free to modify it based on your specific setup and commands!