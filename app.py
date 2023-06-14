from flask import Flask,request,render_template
import csv

# def get_data(filename):
#     with open(filename, 'r') as f:
#         reader = csv.reader(f)
#         data = list(reader)
#     rows = []
#     for row in data:
#         rows.append('<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(row[0], row[1], row[2], row[3]))
#     html = '<table>{}</table>'.format(''.join(rows))
#     return html


def read_csv(filename):
    data = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data


app = Flask(__name__)

# @app.route('/')
# def index():
#     sort_param = request.args.get('sort')
#     filename = 'data.csv'  # Replace with the name of your CSV file
#     data = read_csv(filename)
#     data = get_data(filename)
#     response = make_response(render_template_string('<html><body>{{ data | safe }}</body></html>', data=data))
#     response.headers['Content-Type'] = 'text/html'
#     print(response)
#     return response


@app.route('/')
def index():
    # Get the sort parameter from the query string
    sort_param = request.args.get('sort')

    # Read the data from the CSV file
    data = read_csv('data.csv')

    # Sort the data based on the sort parameter
    if sort_param == 'asc':
        data.sort(key=lambda x: int(x['column4']))
    elif sort_param == 'desc':
        data.sort(key=lambda x: int(x['column4']), reverse=True)

    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)