

while True:
    import pandas as pd
    df = pd.read_csv('test_file1.csv')
    print("Original Dataframe")
    print(df)
    df['Name'] = df['first name'] + ' ' + df['last name']
    df = df.drop(['first name', 'last name'], axis=1)
    df = df.rename(columns = {'class':'Class'})
    df = df.rename(columns = {'section':'Section'})
    df = df.rename(columns = {'marks':'Marks'})
    df = df.rename(columns = {'grade':'Grade'})
    df = df.iloc[:,[4,0,1,2,3]]
    print("Manipulated Dataframe")
    print(df) 
       
    
