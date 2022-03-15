import csv
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import requests
from flask_celery import make_celery
from forms import SexFilter, RaceFilter

conn = psycopg2.connect("host=localhost dbname=test user=wiktorjaniszewski")
cur = conn.cursor()
cursor = conn.cursor()
cur.execute("""
DROP TABLE IF EXISTS public.demographic_data
""")
conn.commit()


app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://localhost:5432/test'
app.config['SECRET_KEY'] = '1234'

celery = make_celery(app)
db = SQLAlchemy(app)


class DemographicData(db.Model):
    id = db.Column(db.BigInteger, primary_key=True)
    DBN = db.Column(db.String(200))
    School_Name = db.Column(db.String(200))
    Category = db.Column(db.String(200))
    Year = db.Column(db.String(200))
    TotalEnroll = db.Column(db.String(200))
    GradeK = db.Column(db.String(200))
    Grade1 = db.Column(db.String(200))
    Grade2 = db.Column(db.String(200))
    Grade3 = db.Column(db.String(200))
    Grade4 = db.Column(db.String(200))
    Grade5 = db.Column(db.String(200))
    Grade6 = db.Column(db.String(200))
    Grade7 = db.Column(db.String(200))
    Grade8 = db.Column(db.String(200))
    Female = db.Column(db.String(200))
    Proc_Female = db.Column(db.String(200))
    Male = db.Column(db.String(200))
    Proc_Male = db.Column(db.String(200))
    Asian = db.Column(db.String(200))
    Proc_Asian = db.Column(db.String(200))
    Black = db.Column(db.String(200))
    Proc_Black = db.Column(db.String(200))
    Hispanic = db.Column(db.String(200))
    Proc_Hispanic = db.Column(db.String(200))
    Other = db.Column(db.String(200))
    Proc_Other = db.Column(db.String(200))
    White = db.Column(db.String(200))
    Proc_White = db.Column(db.String(200))
    EEL_Spanish = db.Column(db.String(200))
    Proc_EEL_Spanish = db.Column(db.String(200))
    EEL_Chinese = db.Column(db.String(200))
    Proc_EEL_Chinese = db.Column(db.String(200))
    EEL_Bengali = db.Column(db.String(200))
    Proc_EEL_Bengali = db.Column(db.String(200))
    EEL_Arabic = db.Column(db.String(200))
    Proc_EEL_Arabic = db.Column(db.String(200))
    EEL_Haitian = db.Column(db.String(200))
    Proc_EEL_Haitian = db.Column(db.String(200))
    EEL_French = db.Column(db.String(200))
    Proc_EEL_French = db.Column(db.String(200))
    EEL_Russian = db.Column(db.String(200))
    Proc_EEL_Russian = db.Column(db.String(200))
    EEL_Korean = db.Column(db.String(200))
    Proc_EEL_Korean = db.Column(db.String(200))
    EEL_Urdu = db.Column(db.String(200))
    Proc_EEL_Urdu = db.Column(db.String(200))
    EEL_Other = db.Column(db.String(200))
    Proc_EEL_Other = db.Column(db.String(200))
    ELA_Test_Take = db.Column(db.String(200))
    ELA_Lvl_1 = db.Column(db.String(200))
    Proc_ELA_Lvl_1 = db.Column(db.String(200))
    ELA_Lvl_2 = db.Column(db.String(200))
    Proc_ELA_Lvl_2 = db.Column(db.String(200))
    ELA_Lvl_3 = db.Column(db.String(200))
    Proc_ELA_Lvl_3 = db.Column(db.String(200))
    ELA_Lvl_4 = db.Column(db.String(200))
    Proc_ELA_Lvl_4 = db.Column(db.String(200))
    ELA_Lvl_34 = db.Column(db.String(200))
    Proc_ELA_Lvl_34 = db.Column(db.String(200))
    Math_Test_Take = db.Column(db.String(200))
    Math_Lvl_1 = db.Column(db.String(200))
    Math_ELA_Lvl_1 = db.Column(db.String(200))
    Math_Lvl_2 = db.Column(db.String(200))
    Math_ELA_Lvl_2 = db.Column(db.String(200))
    Math_Lvl_3 = db.Column(db.String(200))
    Math_ELA_Lvl_3 = db.Column(db.String(200))
    Math_Lvl_4 = db.Column(db.String(200))
    Math_ELA_Lvl_4 = db.Column(db.String(200))
    Math_Lvl_34 = db.Column(db.String(200))
    Math_ELA_Lvl_34 = db.Column(db.String(200))


db.create_all()


# function which enables downloading thr file
def download_file(url, filename=''):
    try:
        if filename:
            pass
        with requests.get(url) as req:
            with open(filename, 'wb') as f:
                for chunk in req.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return filename
    except Exception as e:
        print(e)
        return None


downloadLink = 'https://data.cityofnewyork.us/api/views/7yc5-fec2/rows.csv?accessType=DOWNLOAD'


# celery task which is responsible for downloading and inserting CSV file into our DB
@celery.task(name='download_and_insert')
def insert_data():
    # downloading file
    download_file(downloadLink, 'dataset.csv')

    # adding ID column
    with open('dataset.csv', 'r') as f, open('dataset_id.csv', 'w') as out:
        reader = csv.reader(f)
        writer = csv.writer(out, delimiter=',')
        writer.writerow(['ID'] + next(reader))
        writer.writerows([i] + row for i, row in enumerate(reader, 1))

    # inserting CSV file into our DB
    with open('dataset_id.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO demographic_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                row
            )
    conn.commit()
    return 'Done'


@app.route('/', methods=['GET'])
def welcome():
    insert_data.delay()
    return render_template('welcome.html')


@app.route('/filter', methods=['GET'])
def filter_starting_page():
    return render_template('filter.html')


@app.route('/filter_base_category', methods=['POST', 'GET'])
def filter_category():
    if request.method == 'POST':
        category_filter = request.form['category_filter']
        filtered_base = DemographicData.query.filter_by(Category=category_filter).all()
        return render_template('filtered_base.html', base=filtered_base)
    else:
        return render_template('filter_base_category.html')


@app.route('/filter_base_sex', methods=['POST', 'GET'])
def filter_sex():
    form = SexFilter()
    if form.validate_on_submit():
        filtered_sex = request.form['sex']
        value_1 = request.form['value_1']
        value_2 = request.form['value_2']
        if filtered_sex == 'Female':
            filtered_base = DemographicData.query.filter(DemographicData.Female.between(value_1, value_2)).all()
        elif filtered_sex == 'Male':
            filtered_base = DemographicData.query.filter(DemographicData.Male.between(value_1, value_2)).all()
        return render_template('filtered_base.html', base=filtered_base)
    else:
        return render_template('filter_base_sex.html', form=form)


@app.route('/filter_base_race', methods=['POST', 'GET'])
def filter_race():
    form = RaceFilter()
    if form.validate_on_submit():
        race = request.form['race']
        value_1 = request.form['value_1']
        value_2 = request.form['value_2']
        if race == 'Asian':
            filtered_base = DemographicData.query.filter(DemographicData.Asian.between(value_1, value_2)).all()
        elif race == 'Black':
            filtered_base = DemographicData.query.filter(DemographicData.Black.between(value_1, value_2)).all()
        elif race == 'White':
            filtered_base = DemographicData.query.filter(DemographicData.White.between(value_1, value_2)).all()
        elif race == 'Other':
            filtered_base = DemographicData.query.filter(DemographicData.Other.between(value_1, value_2)).all()
        return render_template('filtered_base.html', base=filtered_base)
    else:
        return render_template('filter_base_race.html', form=form)


if __name__ == '__main__':
    app.run(debug=True, port=8000)


