# Movies-ETL

The code submitted is a Python function that automates the ETL process for three databases inputted as csv and json files. 

The assumptions for the code are:

1 - The three inputs are to two cvs and one json file paths. If one of the file paths or file types doesn't match the assumption, the user will get the message "please enter your file path with this format f'{file_dir}/file_name".

2 - The budget, id and popularity columns from kaggle database can be converted to integer and numeric datatypes and the release date column can be converted to datetype. If the values in these columns are not convertable to the desired datatypes, the user will recieve the message "Caution! Datatype not convertable."

3 - The user has entered the right password for the SQL database. If the password is wrong, the user will receive the message "Did you put in the right password?".
