# Real-Time Twitter Sentiment Analysis Dashboard

## Overview
This project is a real-time sentiment analysis platform for Twitter data, leveraging Big Data technologies. It features a modern Django dashboard for visualization and management, Apache Spark for scalable data processing and machine learning, and Apache Kafka for real-time data streaming.

## Features
- Real-time ingestion and analysis of Twitter data
- Sentiment classification using a trained Logistic Regression model
- Interactive Django dashboard with modern UI
- Data visualization and classification tools
- Modular code for easy extension and deployment

## Project plan

![alt text](/assets/image.png)

[Dataset](https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis) used for training the model

## Directory Structure
```
├── Django-Dashboard/
│   ├── BigDataProject/         # Django project settings
│   ├── dashboard/             # Django app for dashboard UI and logic
│   ├── static/                # Static files (CSS, JS, images)
│   ├── templates/             # HTML templates
│   ├── bigData_logReg_model.pkl/ # Trained ML model (Spark format)
│   ├── db.sqlite3             # Local database (for dev)
│   └── manage.py              # Django management script
├── pySpark_Kafka/
│   ├── kafkaProducer.py       # Kafka producer for Twitter data
│   ├── kafkaConsumer.py       # Kafka consumer for Spark
│   ├── twitter_validation.csv # Validation data
│   └── bigData_logReg_model.pkl/ # Model copy (if needed)
├── pySpark_ML/
│   ├── model_training.ipynb   # Jupyter notebook for model training
│   ├── datasets/              # Training/validation datasets
│   └── bigData_logReg_model.pkl/ # Model artifacts
├── myenv/                     # Python virtual environment (not for version control)
├── zk-single-kafka-single.yml  # Docker Compose for Kafka/ZooKeeper
├── LICENSE
├── README.md
```

## Setup & Installation
1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd spark-twitter-analysis
   ```
2. **Create and activate a virtual environment:**
   ```sh
   python -m venv myenv
   myenv\Scripts\activate  # On Windows
   # source myenv/bin/activate  # On Linux/Mac
   ```
3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
   *(Create `requirements.txt` with Django, pyspark, kafka-python, etc.)*
4. **Set up Kafka and ZooKeeper:**
   - Use the provided `zk-single-kafka-single.yml` with Docker Compose:
     ```sh
     docker-compose -f zk-single-kafka-single.yml up
     ```
5. **Configure Twitter API keys:**
   - Add your Twitter API credentials in the appropriate place in `kafkaProducer.py`.

## Running the Project
### 1. Start Kafka and ZooKeeper
```
docker-compose -f zk-single-kafka-single.yml up
```

### 2. Start the Kafka Producer
```
cd pySpark_Kafka
python kafkaProducer.py
```

### 3. Start the Kafka Consumer (Spark Streaming)
```
python kafkaConsumer.py
```

### 4. Run the Django Dashboard
```
cd Django-Dashboard
python manage.py migrate
python manage.py runserver
```
Visit [http://127.0.0.1:8000/](http://127.0.0.1:8000/) in your browser.

## Model Training
- Use the Jupyter notebook in `pySpark_ML/model_training.ipynb` to train or retrain the sentiment analysis model.
- The trained model is saved in Spark ML format in `bigData_logReg_model.pkl/`.

## Customization & Extending
- Update the dashboard UI in `Django-Dashboard/templates/` and `static/css/style.css`.
- Add new features or visualizations in the `dashboard` Django app.
- Replace or retrain the ML model as needed.

## Screenshots
![alt text](/assets/image-1.png)
MongoDB Compass console

![alt text](/assets/image-2.png)
Data visualisations

![alt text](/assets/image-3.png)
Sentiment classification


## License
This project is licensed under the MIT License. See `LICENSE` for details.

## Acknowledgments
- Apache Spark, Apache Kafka, Django, Twitter API
- FontAwesome, Google Fonts

---
*For questions or contributions, please open an issue or pull request.*