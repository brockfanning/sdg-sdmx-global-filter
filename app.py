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
            series_messages = get_series_messages(series_key, constraints, dsd)
            if len(series_messages) == 0:
                global_serieses[series_key] = observations
                num_series += 1
            else:
                messages = messages + series_messages
                num_removed += 1
        global_dataset = model.StructureSpecificTimeSeriesDataSet(series=global_serieses)
        global_datasets.append(global_dataset)
    msg.data = global_datasets
    messages = get_unique_messages(messages)
    return {
        'messages': messages,
        'sdmx': msg,
        'num_series': num_series,
        'num_removed': num_removed,
    }

def get_series_messages(series_key, constraints, dsd):
    messages = []
    # First look for problems in dimension/attribute codes.
    for dimension in dsd.dimensions:
        if dimension.id in series_key.values and dimension.local_representation is not None and dimension.local_representation.enumerated is not None:
            code = series_key.values[dimension.id].value
            if code not in dimension.local_representation.enumerated:
                messages.append('In "{}", "{}" is not in the global codelist.'.format(dimension.id, code))
    for attribute in dsd.attributes:
        if attribute.id in series_key.values and attribute.local_representation is not None and attribute.local_representation.enumerated is not None:
            code = series_key.values[attribute.id].value
            if code not in attribute.local_representation.enumerated:
                messages.append('In "{}", "{}" is not in the global codelist.'.format(attribute.id, code))
    return messages

def get_content_constraints():
    constraints_path = os.path.join(os.path.dirname(__file__), 'content_constraints.csv')
    constraints = pd.read_csv(constraints_path, encoding_errors='ignore')
    return constraints

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

# Remove rows of data that do not comply with the global SDMX content constraints.
def enforce_global_content_constraints(self, rows, indicator_id):
    before = len(rows.index)
    # Until these constraints are published, we use a local file.
    constraints_path = os.path.join(os.path.dirname(__file__), 'sdmx_global_content_constraints.csv')
    constraints = pd.read_csv(constraints_path, encoding_errors='ignore')
    series_constraints = {}
    matching_rows = []
    skip_reasons = []
    for _, row in rows.iterrows():
        if 'SERIES' not in row:
            continue
        series = row['SERIES']
        if series in series_constraints:
            series_constraint = series_constraints[series]
        else:
            series_constraint = constraints.loc[constraints['SERIES'] == series]
            series_constraints[series] = series_constraint
        if series_constraint.empty:
            continue
        row_matches = True
        ignore_columns = ['SERIES', 'Name']
        for column in series_constraint.columns.to_list():
            if column in ignore_columns:
                continue
            column_constraint = series_constraint[column].iloc[0]
            if column_constraint == 'ALL':
                continue
            allowed_values = column_constraint.split(';') if ';' in column_constraint else [column_constraint]
            if '0' in allowed_values:
                allowed_values.append(0)
            if column not in row and '_T' not in allowed_values:
                row_matches = False
                reason = 'Column "' + column + '" is missing value. Allowed values are: ' + ', '.join(allowed_values)
                if reason not in skip_reasons:
                    skip_reasons.append(reason)
            elif column in row and row[column] not in allowed_values:
                if pd.isna(row[column]) and '_T' in allowed_values:
                    pass
                else:
                    row_matches = False
                    reason = 'Column "' + column + '" has invalid value "' + str(row[column]) + '". Allowed values are: ' + ', '.join(allowed_values)
                    if reason not in skip_reasons:
                        skip_reasons.append(reason)
        if row_matches:
            matching_rows.append(row)

    empty_df = pd.DataFrame(columns=rows.columns)
    constrained_df = empty_df.append(matching_rows)

    if len(skip_reasons) > 0:
        after = len(constrained_df.index)
        message = '{indicator_id} - Removed {difference} rows while constraining data to the global content constraints (out of {total}). Reasons below:'
        difference = str(before - after)
        self.warn(message, indicator_id=indicator_id, difference=difference, total=before)
        for reason in skip_reasons:
            self.warn('  ' + reason)

    return constrained_df


if __name__ == '__main__':
    app.run(debug=True)
