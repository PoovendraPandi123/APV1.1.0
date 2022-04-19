from waitress import serve

from Sources.wsgi import application

if __name__ == "__main__":
    serve(application, host="localhost", port="50003")