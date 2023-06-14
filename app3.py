from flask import Flask, render_template, request
import csv

app = Flask(__name__)

@app.route('/')
def index():
    # Read CSV data from file
    with open('data.csv') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # Sort rows by column 4
    rows = sorted(rows, key=lambda x: int(x[3]))

    # Check if sort parameter is present in request
    sort = request.args.get('sort')
    if sort:
        if sort == 'desc':
            rows.reverse()

    # Render template with sorted rows
    return render_template('index.html', rows=rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
