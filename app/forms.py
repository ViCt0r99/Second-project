from flask_wtf import FlaskForm
from wtforms import StringField, SelectField
from wtforms.validators import InputRequired, DataRequired


class SexFilter(FlaskForm):
    value_1 = StringField('Enter first one', validators=[DataRequired(message="Enter value")])
    value_2 = StringField('Enter second one', validators=[DataRequired(message="Enter value")])
    sex = SelectField('Select sex',
                      [DataRequired(message="Enter value")],
                      choices=[('Male', 'Male'), ('Female', 'Female')])


class RaceFilter(FlaskForm):
    value_1 = StringField('Enter first one', validators=[DataRequired(message="Enter value")])
    value_2 = StringField('Enter second one', validators=[DataRequired(message="Enter value")])
    race = SelectField('Select race',
                       [DataRequired(message="Choose value")],
                       choices=[('Asian', 'Asian'), ('Black', 'Black'),
                                ('White', 'White'), ('Other', 'Other')])
