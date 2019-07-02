from doctest import debug
import pic2char


from flask import Flask

app = Flask(__name__)


@app.route('/hello')
def hello_world():
    pic2char.func()


if __name__ == "__main__":
    app.run(debug == True)
