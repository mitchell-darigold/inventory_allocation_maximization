import pandas as pd


df = pd.read_csv('S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\inventory.csv')

##print(df.head())

#create a dictionary
#eventually I will want to write SQL query that pulls from the item master table and generates a dictionary automatically


allowed_specs = ['3002A', '3007DRYGUM', '3007FO', '3013A', '3025S', '3035A', '9999E', '9999A', '9999E']

df['spec_listed'] = df['SPEC'].str.split('-')


def filter_specs(df, column_name, allowed_specs):
    df[column_name] = df[column_name].apply(lambda x: [val for val in x if val in allowed_specs])
    return df

df = filter_specs(df, 'spec_listed', allowed_specs)

df['CLEANED_SPEC'] = df['spec_listed'].apply(lambda x: '-'.join(map(str,x)))

df = df.drop('spec_listed', axis=1)

df.to_csv('spec_test.csv',index=False)