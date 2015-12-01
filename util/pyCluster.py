import pandas as pd, numpy as np
from sklearn.cluster import DBSCAN

        
def dbscan(dataframe, eps=2, min_samples=10, second_layer_screen=100, zboost=1.2):
    # OPTIONAL, taller building perform better. Distort the feature space to aid clustering.
    if zboost !=1:
        dataframe['z'] = dataframe['z']**zboost
        dataframe.replace([np.inf, -np.inf], np.nan)
        dataframe.dropna(inplace=True)
        dataframe.reset_index()

    # First pass clusters
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(dataframe[['x', 'y', 'z']].as_matrix())
    df = pd.DataFrame([dataframe.x, dataframe.y, dataframe.id, db.labels_]).T
    df.columns = ['x', 'y','id','label'];
    
    # Split the datasets into buildings and not buildings based on label counts
    blds = df.groupby('label').filter(lambda x: len(x) > second_layer_screen and len(x) < 10000);
    blds.reset_index(inplace=True);
    not_blds = df.groupby('label').filter(lambda x: len(x) <= second_layer_screen or len(x) >= 10000);
    not_blds.reset_index(inplace=True)

    # Recluster buildings in only x-y to negate the effects of funny-shaped rooves.
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(blds[['x', 'y']].as_matrix())
    blds = pd.DataFrame([blds.id, db.labels_]).T
    blds.columns = ['id', 'label'];
    blds['id'] = blds['id'].astype(int)

    # Is as building: id, label
    # Not a building: id
    return blds, not_blds['id']
        

                
if __name__ == '__main__':
    import os
    from pgMethods import Methods
    m = Methods(projection=32610);
    df = m.pg_df('SELECT x, y, z, id FROM lidar_nyc_small')

    print 'df loaded'
    #df = pd.read_csv('../tests/data/CrossValidationData.csv')
    blds, not_blds = dbscan(df)
    
    m.df_pg(blds, 'temp12345678');
    m.pg_post('DROP TABLE IF EXISTS checkcheck; CREATE TABLE checkcheck AS SELECT a.* FROM z_20130805_usgsnyc14_18TWL835075 a RIGHT JOIN temp12345678 b ON a.id=b.id;')
    m.pg_post('DROP TABLE temp12345678;')



    
