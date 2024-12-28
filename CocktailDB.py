import json
import sqlite3
import string

import requests
import pandas as pd

# 1. Data Ingestion
api_url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?"

# Query the API for all the cocktails, by starting letter
# Store the results in a Pandas dataframe
drinks_df = pd.DataFrame()

for letter in string.ascii_lowercase:
    response = requests.get(api_url + f"f={letter}")
    if response.ok:
        data = json.loads(response.text)
        temp_df = pd.DataFrame(data["drinks"])
        drinks_df = pd.concat([drinks_df, temp_df])

print(drinks_df.__len__)
# Goal Met

# 2. Data Transformation
''' Schema
Drinks(DrinkID INTEGER PK, DrinkName TEXT, Category TEXT, Alcoholic TEXT, Glass TEXT, Instructions TEXT)
Ingredients(DrinkID INTEGER PK, Ingredient TEXT, Measure TEXT)
This is the normalized schema in order to minimize the number of null values. 
'''

cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions', "strIngredient1", "strIngredient2",
             "strIngredient3", "strIngredient4", "strIngredient5", "strIngredient6", "strIngredient7", "strIngredient8",
             "strIngredient9", "strIngredient10", "strIngredient11", "strIngredient12", "strIngredient13",
             "strIngredient14", "strIngredient15", "strMeasure1", "strMeasure2", "strMeasure3", "strMeasure4",
             "strMeasure5", "strMeasure6", "strMeasure7", "strMeasure8", "strMeasure9", "strMeasure10", "strMeasure11",
             "strMeasure12", "strMeasure13", "strMeasure14", "strMeasure15"]
# Drop the other columns
drinks_df = drinks_df[cols_keep]

ingredients_df = pd.DataFrame()
for index, row in drinks_df.iterrows():
    drink_id = row['idDrink']
    ingredients = row[[f"strIngredient{i}" for i in range(1, 16)]]
    measures = row[[f"strMeasure{i}" for i in range(1, 16)]]
    ingredients.dropna(inplace=True)
    ingredients = ingredients.to_list()
    measures.dropna(inplace=True)
    measures = measures.to_list()
    data = {
        'drinkID': drink_id,
        'ingredient': ingredients,
        'measure': measures
    }
    if len(measures) != len(ingredients):   # Edge Case
        print("Skipping ID " + drink_id)
        # drinks_df.drop(drinks_df[drinks_df["idDrink"] == drink_id].index, inplace=True)
        continue
    result = pd.DataFrame(data)
    ingredients_df = pd.concat([ingredients_df, result])

ingredients_df.reset_index()

cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions']
# Drop the other columns
drinks_df = drinks_df[cols_keep]
# 3. Edge Case Handling
# Remove the drinks we skipped in line 56


# 4. Dump to SQL
conn = sqlite3.connect('CocktailDB')
drinks_df.to_sql('Drinks', conn, if_exists='replace')
ingredients_df.to_sql('Ingredients', conn, if_exists='replace')
conn.commit()
conn.close()
