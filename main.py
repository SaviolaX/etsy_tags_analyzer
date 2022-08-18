import pandas as pd
from flask import (Flask, render_template, make_response, request, redirect,
                   url_for)

from services import (convert_to_csv_and_save_to_aws,
                      join_tables_and_save_to_aws, get_unique_tags,
                      delete_all_temporary_csv_files)
from settings import s3, BUCKET_NAME, SECRET_KEY

app = Flask(__name__)
app.config['SECTER_KEY'] = SECRET_KEY


@app.route('/', methods=['GET', 'POST'])
def get_files_convert_to_csv_save_to_aws():

    # delete all temporary csv files in s3 bucket
    try:
        delete_all_temporary_csv_files()
    except:
        pass

    # get files from input
    if request.method == 'POST':

        my_shop_table = request.files.get('my_shop_file')
        another_shops_tables = request.files.getlist('another_shop_files')

        # check if excel files are be able to be edited
        try:
            # convert all input objects to csv and save to aws
            convert_to_csv_and_save_to_aws(input_object=my_shop_table,
                                           input_list=another_shops_tables)

            # join all tables from another_shop_tables in one table
            # and save to aws
            join_tables_and_save_to_aws()

            return redirect(url_for('result_table'))
        except TypeError as ex:
            return render_template('file_error.html', exception=ex)

    return make_response(render_template('index.html'))


@app.route('/result_table', methods=['GET', 'POST'])
def result_table():

    # get file path
    path_to_object = get_unique_tags()

    # get file object from s3
    unique_tags_table = s3.get_object(Bucket=BUCKET_NAME, Key=path_to_object)
    # read file and create dataframe
    result_table = pd.read_excel(unique_tags_table['Body'].read())
    # convert dataframe to html table
    html = result_table.to_html()
    # get clear filename
    filename = path_to_object.split('/')[1]
    # download link straight from s3 bucket
    download_path = f'https://{BUCKET_NAME}.s3.eu-central-1.amazonaws.com/{path_to_object}'

    return render_template('result_page.html',
                           html_table=html,
                           filename=filename,
                           download_url=download_path)


if __name__ == '__main__':
    app.run(port=5000, debug=False)
