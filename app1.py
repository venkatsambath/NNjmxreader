from flask import Flask, render_template
import csv

app = Flask(__name__)

@app.route('/')
def index():
    # read the CSV file
    with open('data.csv') as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
        print(rows)

    # sort the rows based on the 4th column (count)
    sorted_rows = sorted(rows, key=lambda x: int(x['column4']))

    # pass the sorted rows to the template for rendering
    return render_template('index.html', rows=sorted_rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
