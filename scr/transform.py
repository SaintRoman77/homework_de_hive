import pandas as pd


def add_year(df):
    df['Subscription Year'] = pd.DatetimeIndex(df['Subscription Date']).year
    return df


def add_group(df):
    df['Group'] = [index // (int(len(df)) // 10) + 1 for index in range(int(len(df)))]
    return df


if __name__ == '__main__':
    df_customers = pd.read_csv('../csv_files/customers.csv')
    add_year(df_customers)
    add_group(df_customers)
    df_customers.to_csv('../csv_files/upd/customers.csv', index=False)
    df_organizations = pd.read_csv('../csv_files/organizations.csv')
    add_group(df_organizations)
    df_organizations.to_csv('../csv_files/upd/organizations.csv', index=False)
    df_people = pd.read_csv('../csv_files/people.csv')
    add_group(df_people)
    df_people.to_csv('../csv_files/upd/people.csv', index=False)
