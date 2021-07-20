<!-- ABOUT THE PROJECT -->
## About The Project

This is an importer, which get data from Twitter, using it's API and store them in a Mongo db. You can get both user's timeline and different hashtags.
The importer will be run daily to get new data.

## Built With

* [Apachi Airflow](https://airflow.apache.org/)
* [Mongodb](https://www.mongodb.com/)
* [Docker](https://www.docker.com/)


<!-- GETTING STARTED -->
## Getting Started

Clone the repo in your local machine.

### Prerequisites

* Install Docker Desktop
* Get Twitter Developer API Access
* Make sure you set up vpn if Twitter is banned in your area

<!-- USAGE EXAMPLE -->
## Usage

Run `docker-compose up` and airflow will be accessible at `localhost:8080/admin/`
