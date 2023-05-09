setup:
	python3 -m venv ./env \
    && source ./env/bin/activate \
    && pip install --upgrade pip \
    && pip install -r requirements.txt
sparksubmit:
	zip -r movie_ratings.zip data/ movie_ratings/ env/lib/python3.11/site-packages/ --exclude='*.pyc'