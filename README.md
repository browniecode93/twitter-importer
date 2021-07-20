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

Add Twitter API keys:
https://github.com/browniecode93/twitter-importer/blob/3ef24a25d87fd5e97c4891594a9763fac73934b3/dags/get_hashtags.py#L10

Modify list of usernames:
https://github.com/browniecode93/twitter-importer/blob/3ef24a25d87fd5e97c4891594a9763fac73934b3/dags/get_user_timeline.py#L119

Modify list of hashtags:
https://github.com/browniecode93/twitter-importer/blob/3ef24a25d87fd5e97c4891594a9763fac73934b3/dags/get_hashtags.py#L121

_For more information about the project, please refer to [Documentation](https://medium.com/)_

_For more information about airflow image, please refer to [docker-airflow](https://github.com/puckel/docker-airflow)_
