# Processing Notes

## Setup steps

* Setup python and pip
* Clone repo
* Create venv: `python -m venv venv`
* Install requirements: `pip install -r requirements.txt`

## Running Analysis

* Navigate to project
* `source venv/bin/activate`
* `./builder.py`
* Change to the data directory
* Checksum the data: `find . -type f -not -path "./data/*" -exec sha256sum {} \; > files.sha256sum`

## Upload Results

* Ensure you are in the right data folder
* `aws --profile=r2 --endpoint *Cloudflare Endpoint* s3 sync . s3://pappin-assets/bslTools/*update date*/ --exclude 'data/*' --exclude 'README.md'`
* Add the readme for the data folder to the git repo for human access
* `git add README.md`
* `git commit -m "Updated bslTools for data as of *enterDate*`

## Cleanup bulk data

* Be sure to delete the data folder to make sure the storage charges stay low