# NYC Taxi Data Pipeline

An automated batch and streaming data pipeline over NYC Yellow Taxi trip records, running
locally with Docker: Spark for compute, Kafka for the event stream, Airflow for
orchestration, ClickHouse as the serving layer, Power BI for the dashboard.

---

## Disclaimer

### **This project is done for learning purpose**

#### **1. Spark is overkill for the volume of data used.**
The amount of data sits comfortably on one machine and every transformation
can be expressed in SQL. Pandas and MotherDuck/DuckDB would be faster with
a fraction of the setup. Spark was chosen for skill development.

#### **2. No live data stream actually happens.**
NYC TLC doesn't publish live feed, so the event stream is a replay of historical files. 
Kafka was chosen to learn streaming architecture.

#### **3. ClickHouse is not load-bearing at this volume.** 
Power BI could read the gold data (very small) directly. It exists to give the
pipeline a queryable endpoint rather than terminating in files.

#### **4. Documentation of debugging and limitations**
These exist for future learning reference.

---

## Tech stack

PySpark 4.1.2 · Apache Kafka 4.3.1 (KRaft) · Apache Airflow 3.3.0 · PostgreSQL 16 · ClickHouse
26.3 · Docker Compose · Power BI

---

## Architecture

![image](https://github.com/darrenrio529-beep/nyc-taxi-pipeline/blob/f8a20d57f0518f582f4e8ead52e5e6e7833a393b/architecture.png)

Twelve containers inside Docker. Producer simulate data stream to Kafka. Kafka feeds two independent consumers, 
one writing into bronze and the other compute 15-minute sliding-window net flow per zone to simulate live update on dashboard.

For further explanation regarding architecture refer to: [docs/DESIGN.md](docs/DESIGN.md)

---

## Running it

For full setup and sample data, refer to: [docs/SETUP.md](docs/SETUP.md)

---

## Documentation

- **[docs/DESIGN.md](docs/DESIGN.md)**
- **[docs/SETUP.md](docs/SETUP.md)**
- **[docs/DEBUGGING.md](docs/DEBUGGING.md)** --- notable debugging note
- **[docs/LIMITATIONS.md](docs/LIMITATIONS.md)** --- limitations to be resolved in the future
