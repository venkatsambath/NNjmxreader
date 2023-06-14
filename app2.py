from flask import Flask, render_template
import csv

app = Flask(__name__)

@app.route('/')
def index():
    with open('data.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        rows = [{'timestamp': row[0], 'operation': row[1], 'user': row[2], 'count': int(row[3])} for row in reader]

    sorted_rows = sorted(rows, key=lambda x: x['count'])

    return render_template('index.html', rows=sorted_rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)