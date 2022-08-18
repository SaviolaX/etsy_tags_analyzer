import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime

from settings import s3, BUCKET_NAME, aws_credentials


def delete_all_temporary_csv_files():
    """
    Delete all csv files
    """
    # iterate all csv files in bucket
    for key in s3.list_objects(Bucket=BUCKET_NAME,
                               Prefix='temp_csv/')['Contents']:
        # delete each found csv file
        s3.delete_object(Bucket=BUCKET_NAME, Key=key['Key'])
        filename = key['Key'].split('/')[1]
        print(f'[INFO] {filename} was deleted successfully.')


def get_unique_tags():
    """
    Find unique tags in joined table of another shops
    that are not in my_shop table.
    Sort and save as excel file to aws
    """
    # get needed tables from aws
    my_shop = s3.get_object(Bucket=BUCKET_NAME, Key='temp_csv/my_shop.csv')
    joined_table = s3.get_object(Bucket=BUCKET_NAME,
                                 Key='temp_csv/joined_table.csv')
    # create table dataframes
    my_df = pd.read_csv(my_shop['Body'])
    del my_df['Tag Occurrences']
    joined_df = pd.read_csv(joined_table['Body'])
    # inner join 2 tables by 'Tags' column
    common = joined_df.merge(my_df, on='Tags')
    # get unique values that are not in my_shop table
    unique = joined_df[(~joined_df['Tags'].isin(common['Tags']))]
    # sort table by needed column
    sorted_table = unique.sort_values('Average Searches (US)', ascending=False)
    # create excel table as bytes object
    xlsx_buffer = BytesIO()
    df = pd.DataFrame(sorted_table)
    df.to_excel(xlsx_buffer, index=None, header=True)
    # save excel object to aws
    path_to_object = save_xlsx_to_aws(xlsx_buffer=xlsx_buffer)
    return path_to_object


def join_tables_and_save_to_aws():
    """
    :Iterate objects in bucket
    :Create dataframes and add them to a list
    :Concatenate tables from the list into a single table
    :Remove duplicates
    :Remove unnessesary column - 'Tag Occurrences'
    :Save to aws as csv file
    """
    # list of dataframes
    frames = []
    # iterate objects in bucket, get dataframes and append them to list
    for key in s3.list_objects(Bucket=BUCKET_NAME,
                               Prefix='temp_csv/')['Contents']:
        file_loc = key['Key']
        if file_loc.split('/')[1] != 'my_shop.csv':
            df = pd.read_csv(f"s3://{BUCKET_NAME}/{file_loc}",
                             storage_options=aws_credentials,
                             encoding='utf-8')
            frames.append(df)
    # concatenate all tables from frames list into one table
    concated_lists = pd.concat(frames, axis=0, ignore_index=True)
    # remove column
    del concated_lists['Tag Occurrences']
    # remove duplicates
    no_duplicates_table = concated_lists.drop_duplicates()
    # create csv obj in bytes and save one to aws
    csv_buffer = StringIO()
    no_duplicates_table.to_csv(csv_buffer, index=None)
    save_csv_to_aws(csv_buffer=csv_buffer, name='joined_table')


def convert_to_csv_and_save_to_aws(input_object, input_list: list) -> None:
    """
        input_object: excel file
        input_list: list of excel files
    :Convert excel file to csv and save one as bytes obj.
    :Create empty csv file on aws and fill it up with decoded bytes obj
    """
    # convert my_shop_table to csv and save to aws
    my_shop_in_bytes = convert_to_csv(input_object)
    save_csv_to_aws(my_shop_in_bytes, name='my_shop')

    # convert another_shops_tables to csv and save to aws
    counter = 1
    for shop_table in input_list:
        another_tables_in_bytes = convert_to_csv(shop_table)
        save_csv_to_aws(another_tables_in_bytes, name=f'table_{counter}')
        counter += 1


def convert_to_csv(excel_file) -> bytes:
    """
        excel_file: file in any excel format
    :Convert excel file to csv file in bytes
    :Return bytes obj
    """
    file = pd.read_excel(excel_file)
    df = pd.DataFrame(file)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=False, index=None)
    return csv_buffer


def save_csv_to_aws(csv_buffer: bytes, name: str) -> None:
    """
        csv_buffer (bytes): csv file in bytes
        name (str): name for csv file on aws
    :Use aws s3 client to create csv file on aws
    :and fill it up with decoded data from csv_buffer
    """
    s3.put_object(Bucket=BUCKET_NAME,
                  Key=f'temp_csv/{name}.csv',
                  Body=csv_buffer.getvalue())


def save_xlsx_to_aws(xlsx_buffer: bytes) -> None:
    """
        xlsx_buffer (bytes): xlsx file in bytes
        name (str): name for xlsx file on aws
    :Use aws s3 client to create xlsx file on aws
    :and fill it up with decoded data from xlsx_buffer
    """
    cur_date = datetime.now().strftime('%d_%m_%Y__%H_%M_%S')
    path = f'result/result_{cur_date}.xlsx'

    s3.put_object(Bucket=BUCKET_NAME, Key=path, Body=xlsx_buffer.getvalue())

    return path