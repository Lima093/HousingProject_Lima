# Name: Farhana Lima
# Purpose: The final, very big "Housing project"

# Installing the important libraries for the project in venv:
# --- pip3 install pandas numpy
# --- pip3 install mysql-connector-python
# Importing pandas for data manipulation.
import pandas as pd
# Importing numpy for numerical operations.
import numpy as np
# Importing random for generating random numbers.
import random
# Importing mysql.connector to establish a connection to the MySQL server
import mysql.connector
# Importing time to give some pause in the console.
import time
# From files.py importing the file path for further operation.
from files import housingFile, incomeFile, zipFile

def main():
    # A nice welcome ( I like this)
    print(f"*".center(42, "*"))
    print(f" Welcome to Housing Project ".center(42, "*"))
    print(f"*".center(42, "*"))
    # Blank line.
    print()
    # A pause ( just for decoration)
    input(f"Press [ENTER] to continue")
    print()

    # This is a big housing project, I want to separate the whole process into three part.

    # --------------------------------------------- THE CSV FILE PART ------------------------------------------------ #

    # This is for reading data from CSV files into Pandas DataFrames.
    housing_df = pd.read_csv(housingFile)
    income_df = pd.read_csv(incomeFile)
    zip_df = pd.read_csv(zipFile)

    # This function is for cleaning guid column for all three file.
    # It will return True if the length of guid is equal to 36 and all characters are either alphanumeric or hyphen,
    # And will return False if it is not.
    def is_valid_guid(guid):
        return len(guid) == 36 and all(c.isalnum() or c == '-' for c in guid)

    # This will filter rows where 'guid' is a valid GUID in all three file.
    housing_df = housing_df[housing_df['guid'].apply(is_valid_guid)]
    income_df = income_df[income_df['guid'].apply(is_valid_guid)]
    zip_df = zip_df[zip_df['guid'].apply(is_valid_guid)]

    # This function is for zip column.
    # Checks if the length of the provided zip_code string is exactly 5 characters and numeric characters
    def is_valid_zip(zip_code):
        return len(zip_code) == 5 and zip_code.isdigit()

    # To clean the zip code
    def clean_zip_code(row, zip_df, zip_column, city_column):
        # Try except block will handle the zip column if any error occurs.
        try:
            # It will check if the current ZIP code is not valid.
            if not is_valid_zip(row[zip_column]):
                # This will confirm the city column is present and not empty.
                if city_column in row and not pd.isna(row[city_column]):
                    # This will find the close city in the zip dataframe.
                    close_by_city = zip_df.loc[zip_df[city_column] == row[city_column]]
                    # It will check if there are close by cities.
                    if not close_by_city.empty:
                        # Will take the first digit of the close city.
                        first_digit = close_by_city.iloc[0][zip_column][0]
                        # Create a new zip code according to requirement.
                        new_zip_code = f"{first_digit}0000"
                        return new_zip_code
                # If there are no close-by cities, generate a random ZIP code
                return f"{random.randint(1, 9)}0000"
            # If the current ZIP code is valid, return it won't change.
            return row[zip_column]
        # Will handle the errors as exception (e).
        except Exception as e:
            print(f"Error in clean_zip_code: {e}")
            # If any error occur, it will return the original.
            return row[zip_column]

    # These three lines of code are applying the clean_zip_code function to the 'zip_code' column of three DataFrames.
    # The lambda function is used to pass the appropriate arguments to clean_zip_code for each DataFrame.
    housing_df['zip_code'] = housing_df.apply(lambda row: clean_zip_code(row, zip_df, 'zip_code', 'city'), axis=1)
    income_df['zip_code'] = income_df.apply(lambda row: clean_zip_code(row, zip_df, 'zip_code', 'city'), axis=1)
    zip_df['zip_code'] = zip_df.apply(lambda row: clean_zip_code(row, zip_df, 'zip_code', 'city'), axis=1)

    # To define if the housing median age is valid or not.
    # pd.notna function from the pandas library to check if the value of age is not a missing value.
    def is_valid_housing_median_age(age):
        return pd.notna(age) and 10 <= age <= 50

    # This function will clean the housing median age column.
    def clean_housing_median_age(row, housing_median_age_column):
        # The try block attempts to perform the cleaning operations.
        try:
            # This will convert the value to a numerical one (NAN)
            numeric_value_age = pd.to_numeric(row[housing_median_age_column], errors='coerce')
            # Calls the function to check the converted value is valid,
            # if not it will replace a random age value from 10 to 50.
            if not is_valid_housing_median_age(numeric_value_age):
                new_age = random.randint(10, 50)
                # Returns the new age that wasn't valid
                return new_age
            # Will convert the new age to integer.
            return int(numeric_value_age)
        # Catches any exceptions that may occur and print the error massage as e.
        except Exception as e:
            print(f"Error in clean_housing_median_age: {e}")
            # If exception occur it will return none.
            return None

    # This line applies the clean_housing_median_age function and update the column with the cleaned or replaced values
    housing_df['housing_median_age'] = housing_df.apply(lambda row: clean_housing_median_age(row, 'housing_median_age'),
                                                        axis=1)

    # To clean the other numeric value.
    def clean_numeric_value(value, lower_limit, upper_limit):
        # The try block attempts to perform the cleaning operations.
        try:
            # This will convert the value to a numeric value, coercing errors to NaN
            numeric_value = pd.to_numeric(value, errors='coerce')
            # Check if the numeric value is not NaN and within the specified range
            if not np.isnan(numeric_value) and lower_limit <= numeric_value <= upper_limit:
                # Return the cleaned numeric value
                return numeric_value
            else:
                # If not valid, return a random value within the specified range.
                return random.randint(lower_limit, upper_limit)
        # Handle any exceptions that may occur
        except Exception as e:
            print(f"Error in clean_numeric_value: {e}")
            # If exception occur it will return none
            return None

    # Each line will call the clean_numeric_value and update associated column to relevant dataframe.
    # The upper and lower limit is given for each column.
    housing_df['total_rooms'] = housing_df['total_rooms'].apply(lambda value: clean_numeric_value(value, 1000, 2000))
    housing_df['total_bedrooms'] = housing_df['total_bedrooms'].apply(
        lambda value: clean_numeric_value(value, 1000, 2000))
    housing_df['population'] = housing_df['population'].apply(lambda value: clean_numeric_value(value, 5000, 10000))
    housing_df['households'] = housing_df['households'].apply(lambda value: clean_numeric_value(value, 500, 2500))
    housing_df['median_house_value'] = housing_df['median_house_value'].apply(
        lambda value: clean_numeric_value(value, 100000, 250000))
    income_df['median_income'] = income_df['median_income'].apply(
        lambda value: clean_numeric_value(value, 100000, 750000))

    # **** important: you have to make change here ****
    # Save cleaned data to a new CSV file on user's selected path.
    housing_df.to_csv('/Users/PycharmProjects/pythonProject/cleaned_housing.csv', index=False)
    # Save cleaned income data to a new CSV file
    income_df.to_csv('/Users/PycharmProjects/pythonProject/cleaned_income.csv', index=False)
    # Save cleaned zip-city-county-state data to a new CSV file
    zip_df.to_csv('/Users/PycharmProjects/pythonProject13/cleaned_zip_city_county_state.csv', index=False)

    # Three variable to represent file paths for three cleaned csv data.
    cleaned_housing_file = 'cleaned_housing.csv'
    cleaned_income_file = 'cleaned_income.csv'
    cleaned_zip_file = 'cleaned_zip_city_county_state.csv'

    # Read the cleaned CSV files into Pandas DataFrames
    cleanedHousing_df = pd.read_csv(cleaned_housing_file)
    cleanedIncome_df = pd.read_csv(cleaned_income_file)
    cleanedZip_df = pd.read_csv(cleaned_zip_file)

    # Merge all data with the same 'guid' column
    combined_data = pd.merge(cleanedZip_df, cleanedIncome_df, on='guid')
    combined_data = pd.merge(combined_data, cleanedHousing_df, on='guid')

    # Will export combined_data (pandas dataframe) to CSV data.
    combined_data.to_csv('combined_data.csv', index=False)

    # ----------------------------------------------------- MYSQL PART ----------------------------------------------- #

    # Defining database connection parameters
    # type your own MySQL password instead of mypassword and change database name to yours.
    hostname = 'localhost'
    username = 'root'
    password = 'mypassword'
    database_name = 'housing_project'
    csv_file_path = 'combined_data.csv'

    # This time I want to create a connection to the MySQL database
    # The try block starts by attempting to establish the connection.
    try:
        connection = mysql.connector.connect(
            host=hostname,
            user=username,
            password=password,
            database=database_name
        )

        # This will read the CSV file into dataframe
        combined_data = pd.read_csv(csv_file_path)

        # I am creating a cursor to execute SQL commands
        cursor = connection.cursor()

        # This code is for inserting data from Pandas DataFrame (combined_data) into a MySQL database
        # The query is to specify column that will be inserted to database.
        # %s (placeholders) is for the values.
        insert_query = """
                INSERT INTO housing 
                (`guid`, `zip_code`, `city`, `state`, `county`, `housing_median_age`, 
                `total_rooms`, `total_bedrooms`, `population`, `households`, `median_income`, `median_house_value`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        # This loop iterates over each row in the Pandas DataFrame
        for index, row in combined_data.iterrows():
            # Values in the tuple corresponds to the order of placeholders in the SQL INSERT statement.
            values = (
                row['guid'], row['zip_code'], row['city'], row['state'], row['county'],
                row['housing_median_age'], row['total_rooms'], row['total_bedrooms'],
                row['population'], row['households'], row['median_income'], row['median_house_value']
            )
            # This method will method that executes the SQL query and the given value.
            cursor.execute(insert_query, values)

        # This ensures that the changes are permanent and close the cursor and connection
        connection.commit()

    # The except will handel potential error may occur;
    # This occurs when trying to open a file that doesn't exist.
    except FileNotFoundError:
        print(f"File not found: Ensure that the path is correct.")
    # This block catches errors related to MySQL database operations using the mysql.connector module.
    except mysql.connector.Error as e:
        print(f"Error fetching data from the database: {e}")
    # It occurs when trying to read a file with pandas (pd) that is empty.
    except pd.errors.EmptyDataError:
        print(f"Empty data error: Check your file please.")
    # This block catches any other general exceptions that are not handled by the previous blocks.
    except Exception as e:
        print(f"An error occurred: {e}")

    # -------------------------------------- BEGINNING IMPORT & VALIDATION PART -------------------------------------- #

    # The try block attempt to execute the code inside and handle the potential error.
    try:
        # This prints the message "Beginning import" to the console
        print(f"Beginning import")
        # This line introduces a pause in the execution of the script, will sleep for 1 sec
        time.sleep(1)
        print()
        # This indicates that the script is now performing a task related to cleaning data from the housing file.
        print(f"Cleaning Housing File data")
        time.sleep(1)
        # Indicates cleaning income file data and a pause
        print(f"Cleaning income File data")
        time.sleep(1)
        # Same thing for zip file.
        print(f"Cleaning Zip File data")
        time.sleep(1)
        # Indicates the combining and importing process. As I combined all files than imported.
        print(f"Combining 3 files together and importing it to database.")
        time.sleep(1)
        # This block calculates the number of records imported into the database.
        num_records_imported = len(combined_data)
        # Print the record number imported.
        print(f"{num_records_imported} records imported into the database")
        time.sleep(1)
        # Another massage!
        print(f"Import completed")
        time.sleep(1)
        # A blank line and pause and a blank line.
        print()
        input("Press [ENTER] to continue")
        print()

    # If any error occurs!
    except Exception as e:
        print(f"An error occurred: {e}")

    # This will attempt to run validation and calculation
    try:
        # Print validation message.
        print(f"Beginning validation")
        print()
        time.sleep(1)
        # This loop will keep prompting the user until a valid input is provided
        while True:
            try:
                # This will take user input.
                total_rooms_input = input("Total rooms (number): ")
                # Will convert the input to an integer.
                total_rooms = int(total_rooms_input)

                # This line creates an SQL query string using an f-string.
                query = f"SELECT SUM(total_bedrooms) AS total_bedrooms FROM housing WHERE total_rooms > {total_rooms}"
                # It sends the query to the connected database for execution.
                cursor.execute(query)
                # The result is stored
                result = cursor.fetchone()
                total_bedrooms = result[0] if result else 0

                # Will print the massage with calculation result
                print(
                    f"For locations with more than {total_rooms} rooms, there are a total of {total_bedrooms} bedrooms.")
                print()

                # Break out of the loop if the input and query execution are successful
                break
            # If user input any invalid number
            except ValueError:
                print(f"Please enter a valid number for total rooms.")
            # To handle specific exceptions like unexpected termination of input and provide a user-friendly error message.
            except EOFError:
                print(f"That's not like a good user! ok! quitting the program!!")
                # With the error it will break the loop
                break
            # A general exception that may occur. it can capture a wide range of exception.
            except Exception as e:
                print(f"An error occurred: {e}, bye bye!")
                # Same as above.
                break

        # The program continues here after the while loop

        while True:

            try:
                # Take user input for ZIP code
                zipcode_input = input("ZIP code: ")
                zipcode = int(zipcode_input)

                # Execute SQL query to find the median household income for the provided ZIP code
                query = f"SELECT median_income FROM housing WHERE zip_code = {zipcode}"
                cursor.execute(query)
                result = cursor.fetchone()

                if result:
                    median_income = result[0]
                    print(f"The median household income for ZIP code {zipcode} is ${median_income:,}.")
                    break
                else:
                    print(f"Database does not have this ZIP code. Please enter a ZIP code from database.")

            # If user input any invalid number
            except ValueError:
                print(f"Please enter a valid ZIP code.")
            # To handle specific exceptions like unexpected termination of input.
            except EOFError:
                print(f"quitting the program!!")
                # With the error it will break the loop
                break
            # A general exception that may occur. it can capture a wide range of exception.
            except Exception as e:
                print(f"An error occurred: {e}, bye bye!")
                # Same as above.
                break
        # This loop iterates over the rows returned by the executed SQL query.
        for _ in cursor:
            # it does nothing for each iteration
            pass
        # This line closes the database cursor.
        cursor.close()
        # This line closes the database connection.
        connection.close()

        time.sleep(1)
        print()
        # Exit massage.
        print(f"Program exiting!")
        print()
        # A nice thank you massage.
        print(f" THANK YOU ".center(42, "*"))

    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()