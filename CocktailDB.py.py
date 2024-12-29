import json
import sqlite3
import string
import requests
import pandas as pd

# 1. Data Ingestion
api_url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?"
drinks_df = pd.DataFrame()

# Loop over each letter in the alphabet to fetch data for cocktails starting with that letter
for letter in string.ascii_lowercase:
    response = requests.get(api_url + f"f={letter}")
    if response.ok:
        data = json.loads(response.text)
        temp_df = pd.DataFrame(data["drinks"])
        drinks_df = pd.concat([drinks_df, temp_df])

print(f"Total number of cocktails fetched: {drinks_df.__len__()}")

# 2. Data Transformation
cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions',
             "strIngredient1", "strIngredient2", "strIngredient3", "strIngredient4", "strIngredient5",
             "strIngredient6", "strIngredient7", "strIngredient8", "strIngredient9", "strIngredient10",
             "strIngredient11", "strIngredient12", "strIngredient13", "strIngredient14", "strIngredient15",
             "strMeasure1", "strMeasure2", "strMeasure3", "strMeasure4", "strMeasure5", "strMeasure6",
             "strMeasure7", "strMeasure8", "strMeasure9", "strMeasure10", "strMeasure11", "strMeasure12",
             "strMeasure13", "strMeasure14", "strMeasure15"]

# Keep only necessary columns
drinks_df = drinks_df[cols_keep]
ingredients_df = pd.DataFrame()

# Process each drink and extract its ingredients and their measurements
for index, row in drinks_df.iterrows():
    drink_id = row['idDrink']
    ingredients = row[[f"strIngredient{i}" for i in range(1, 16)]].dropna().to_list()
    measures = row[[f"strMeasure{i}" for i in range(1, 16)]].dropna().to_list()

    if len(measures) != len(ingredients):
        print(f"Skipping ID {drink_id} due to mismatch between ingredients and measures.")
        continue

    data = {
        'drinkID': drink_id,
        'ingredient': ingredients,
        'measure': measures
    }
    result = pd.DataFrame(data)
    ingredients_df = pd.concat([ingredients_df, result])

# Reset the index of the ingredients DataFrame
ingredients_df.reset_index(drop=True, inplace=True)

# Keep only relevant columns for drinks data
cols_keep = ['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions']
drinks_df = drinks_df[cols_keep]


# 3. Convert Oz to mL
def convert_to_ml(measure):
    try:
        # Check if the measurement contains "oz"
        if 'oz' in measure.lower():
            # Extract numeric value from the measurement
            numeric_value = ''.join(filter(lambda x: x.isdigit() or x == '.', measure))

            if not numeric_value:  # Check if numeric value is empty
                # Handle non-numeric measures (e.g., 'frozen', 'dash', etc.)
                print(f"Skipping non-numeric measure: '{measure}'")
                return measure  # Return the original non-numeric measure

            # Convert to float and then to mL
            numeric_value = float(numeric_value)
            measure_ml = numeric_value * 29.5735  # 1 oz = 29.5735 mL
            return f"{measure_ml:.2f} mL"  # Return the measurement in mL

        # If the measurement is not in oz, return it as is
        return measure
    except Exception as e:
        # Print a descriptive error message for any unexpected issue
        print(f"Error converting measure '{measure}': {e}")
        return measure


# Apply the conversion function to the 'measure' column
##ingredients_df['measure'] = ingredients_df['measure'].apply(convert_to_ml)

# 4. Save to SQLite Database
conn = sqlite3.connect('CocktailDB')
drinks_df.to_sql('Drinks', conn, if_exists='replace', index=False)
ingredients_df.to_sql('Ingredients', conn, if_exists='replace', index=False)
conn.commit()
conn.close()

print("Data has been successfully processed and saved to the database.")
