import os
from flask import Flask, send_from_directory, request, url_for, render_template
from werkzeug.utils import secure_filename
import pandas as pd
from sdmx import model, read_sdmx, to_xml
from uuid import uuid1
from pathlib import Path

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'xml'}

app=Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def upload_form():
    return render_template('upload.html')

@app.route('/filter', methods=['POST'])
def upload_results():
    constraints = get_content_constraints()
    dsd = get_global_dsd()

    file = request.files['file'] if 'file' in request.files else None

    if file is None or file.filename == '':
        return render_template('results.html',
            messages=['File missing.'])

    if not allowed_file(file.filename):
        return render_template('results.html',
            messages=['File must be .xml.'])

    subfolder = str(uuid1())
    folder = os.path.join(app.config['UPLOAD_FOLDER'], subfolder)
    Path(folder).mkdir(parents=True, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(folder, filename)
    file.save(filepath)
    ret = filter_sdmx(filepath, constraints, dsd)

    if ret['num_series'] > 0 and ret['num_removed'] > 0:
        with open(filepath, 'wb') as f:
            f.write(to_xml(ret['sdmx']))
        return render_template('results.html',
            num_removed=ret['num_removed'],
            num_total=ret['num_removed'] + ret['num_series'],
            messages=ret['messages'],
            download=url_for('download_file', folder=subfolder, name=filename))
    elif ret['num_removed'] == 0:
        return render_template('results.html',
            messages=['The file is already globally-compatible.'])
    elif ret['num_series'] == 0:
        return render_template('results.html',
            messages=['All of the series keys were removed, so there is no output available.'] + ret['messages'])

@app.route('/uploads/<folder>/<name>')
def download_file(folder, name):
    return send_from_directory(os.path.join(app.config["UPLOAD_FOLDER"], folder), name)

def filter_sdmx(filepath, constraints, dsd):
    messages = []
    msg = read_sdmx(filepath)
    num_series = 0
    num_removed = 0
    global_datasets = []
    for dataset in msg.data:
        global_serieses = {}
        for series_key, observations in dataset.series.items():
            series_messages = get_series_messages(series_key, observations, constraints, dsd)
            if len(series_messages) == 0:
                global_serieses[series_key] = observations
                num_series += 1
            else:
                messages = messages + series_messages
                num_removed += 1
        global_dataset = model.StructureSpecificTimeSeriesDataSet(series=global_serieses, structured_by=dsd)
        global_datasets.append(global_dataset)
    msg.data = global_datasets
    messages = get_unique_messages(messages)
    return {
        'messages': messages,
        'sdmx': msg,
        'num_series': num_series,
        'num_removed': num_removed,
    }

def get_series_messages(series_key, observations, constraints, dsd):
    messages = []
    # First look for compatibility with the DSD.
    for dimension in dsd.dimensions:
        if dimension.id in series_key.values and dimension.local_representation is not None and dimension.local_representation.enumerated is not None:
            code = series_key.values[dimension.id].value
            if code not in dimension.local_representation.enumerated:
                messages.append('Codelist: In "{}", "{}" is not in the global codelist.'.format(
                    dimension.id,
                    code,
                ))
    for attribute in dsd.attributes:
        if attribute.id in series_key.values and attribute.local_representation is not None and attribute.local_representation.enumerated is not None:
            code = series_key.values[attribute.id].value
            if code not in attribute.local_representation.enumerated:
                messages.append('Codelist: In "{}", "{}" is not in the global codelist.'.format(
                    attribute.id,
                    code,
                ))

    # Now look for compatibility with content constraints.
    series_code = series_key.values['SERIES'].value
    if series_code in constraints:
        for concept in constraints[series_code]:
            column_constraint = constraints[series_code][concept]
            if column_constraint == 'ALL':
                continue
            allowed_values = column_constraint.split(';') if ';' in column_constraint else [column_constraint]
            if concept not in series_key.values:
                # If it is not in the series key, it might be an attribute.
                # Check the first observation of attributes.
                attrib_key = observations[0].dimension
                if concept not in attrib_key.values:
                    messages.append('Constraints: In series "{}" the concept "{}" is missing. Allowed values are: {}'.format(
                        series_code,
                        concept,
                        ', '.join(allowed_values),
                    ))
                elif attrib_key.values[concept].value not in allowed_values:
                    messages.append('Constraints: In series "{}" the attribute "{}" has a disallowed value "{}". Allowed values are: {}'.format(
                        series_code,
                        concept,
                        attrib_key.values[concept].value,
                        ', '.join(allowed_values),
                    ))
            elif series_key.values[concept].value not in allowed_values:
                messages.append('Constraints: In series "{}" the dimension "{}" has a disallowed value "{}". Allowed values are: {}'.format(
                    series_code,
                    concept,
                    series_key.values[concept].value,
                    ', '.join(allowed_values),
                ))

    return messages

def get_content_constraints():
    constraints_path = os.path.join(os.path.dirname(__file__), 'content_constraints.csv')
    constraints = pd.read_csv(constraints_path, encoding_errors='ignore')
    constraints.drop(columns=['Name'], inplace=True)
    series = {}
    for _, row in constraints.iterrows():
        series_code = row['SERIES']
        other_dimensions = row.to_dict()
        del other_dimensions['SERIES']
        series[series_code] = other_dimensions
    return series

def get_global_dsd():
    dsd_path = os.path.join(os.path.dirname(__file__), 'global_dsd.xml')
    msg = read_sdmx(dsd_path)
    return msg.structure[0]

def get_unique_messages(messages):
    unique = {}
    for message in messages:
        unique[message] = True
    unique = list(unique.keys())
    unique.sort()
    return unique

if __name__ == '__main__':
    app.run(debug=True)
