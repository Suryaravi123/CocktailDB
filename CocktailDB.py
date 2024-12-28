import json
import sqlite3
import string
import requests
import pandas as pd

# 1. Data Ingestion
# API URL for fetching cocktail data
api_url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?"

# Initialize an empty DataFrame to store all cocktail data
drinks_df = pd.DataFrame()

# Loop over each letter in the alphabet to fetch data for cocktails starting with that letter
for letter in string.ascii_lowercase:
    response = requests.get(api_url + f"f={letter}")  # Send request for each letter
    if response.ok:  # If the request was successful
        data = json.loads(response.text)  # Parse the JSON response
        temp_df = pd.DataFrame(data["drinks"])  # Convert the "drinks" list into a DataFrame
        drinks_df = pd.concat([drinks_df, temp_df])  # Append the new data to the main DataFrame

print(f"Total number of cocktails fetched: {drinks_df.__len__()}")  # Print total number of cocktails fetched
# Goal Met: Drinks data successfully fetched and stored in DataFrame

# 2. Data Transformation
# Define the schema for the SQL tables
# Drinks: Contains general information about each drink
# Ingredients: Contains the ingredients for each drink

cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions',
             "strIngredient1", "strIngredient2", "strIngredient3", "strIngredient4", "strIngredient5",
             "strIngredient6", "strIngredient7", "strIngredient8", "strIngredient9", "strIngredient10",
             "strIngredient11", "strIngredient12", "strIngredient13", "strIngredient14", "strIngredient15",
             "strMeasure1", "strMeasure2", "strMeasure3", "strMeasure4", "strMeasure5", "strMeasure6",
             "strMeasure7", "strMeasure8", "strMeasure9", "strMeasure10", "strMeasure11", "strMeasure12",
             "strMeasure13", "strMeasure14", "strMeasure15"]

# Drop columns not needed for the analysis
drinks_df = drinks_df[cols_keep]

# Initialize an empty DataFrame for storing ingredients data
ingredients_df = pd.DataFrame()

# Process each drink and extract its ingredients and their measurements
for index, row in drinks_df.iterrows():
    drink_id = row['idDrink']
    ingredients = row[[f"strIngredient{i}" for i in range(1, 16)]].dropna().to_list()  # Extract and clean ingredients
    measures = row[[f"strMeasure{i}" for i in range(1, 16)]].dropna().to_list()  # Extract and clean measures

    # Edge case: If the number of ingredients does not match the number of measures, skip the drink
    if len(measures) != len(ingredients):
        print(f"Skipping ID {drink_id} due to mismatch between ingredients and measures.")
        continue

    # Create a DataFrame for the ingredients and measures for this drink
    data = {
        'drinkID': drink_id,
        'ingredient': ingredients,
        'measure': measures
    }
    result = pd.DataFrame(data)
    ingredients_df = pd.concat([ingredients_df, result])  # Append to the main ingredients DataFrame

# Reset the index of the ingredients DataFrame
ingredients_df.reset_index(drop=True, inplace=True)

# Clean up the drinks DataFrame to keep only relevant columns
cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions']
drinks_df = drinks_df[cols_keep]

# 3. Edge Case Handling: Ensure no drinks with missing ingredients are processed
# (Handled above by skipping invalid records during iteration)

# 4. Dump to SQL Database
# Create a SQLite database connection
conn = sqlite3.connect('CocktailDB')

# Save the drinks and ingredients DataFrames to SQL tables
drinks_df.to_sql('Drinks', conn, if_exists='replace', index=False)  # Write Drinks table
ingredients_df.to_sql('Ingredients', conn, if_exists='replace', index=False)  # Write Ingredients table

# Commit the changes and close the connection
conn.commit()
conn.close()

# End of script
