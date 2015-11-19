import pandas as pd, numpy as np
from sklearn.cluster import DBSCAN
        
def dbscan(dataframe, eps=2, min_samples=10):
    # First pass clusters
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(dataframe[['x', 'y', 'z']].as_matrix())
    df = pd.DataFrame([dataframe.x, dataframe.y, dataframe.id, db.labels_]).T
    df.columns = ['x', 'y','id','label'];
    
    # Split the datasets into buildings and not buildings based on label counts
    blds = df.groupby('label').filter(lambda x: len(x) > 200 and len(x) < 4000);
    blds.reset_index(inplace=True);
    not_blds = df.groupby('label').filter(lambda x: len(x) <= 200 or len(x) >= 4000);
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
    df = pd.read_csv('/Users/Mike/Google Drive/water_Mike_Olga/Group Project/Data/lidar_SF_small2.csv')
    df.columns = ['x', 'y', 'z', 'id']
    blds, not_blds = dbscan(df)

    sd = Methods(projection=32610)
    sd.hullCallBack(blds, 'lidar_sf__22')
