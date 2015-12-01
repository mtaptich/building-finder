import pandas as pd, numpy as np
from sklearn.cluster import DBSCAN
import sys
sys.path.insert(0, '../util')
from pgMethods import Methods 
from sqlalchemy import create_engine
    
def sign(x):
    if x>0:
        return 1
    else:
        return 0

def dbscan(dataframe, eps=2, min_samples=10, second_layer_screen=200, zboost=1):
    # OPTIONAL, taller building perform better. Distort the feature space to aid clustering.
    if zboost !=1:
        dataframe['z'] = dataframe['z']**zboost
        dataframe.replace([np.inf, -np.inf], np.nan)
        dataframe.dropna(inplace=True)
        dataframe.reset_index()

    # First pass clusters
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(dataframe[['x', 'y', 'z']].as_matrix())
    df = pd.DataFrame([dataframe.x, dataframe.y, dataframe.id, dataframe.is_building, db.labels_]).T
    df.columns = ['x', 'y','id', 'is_building','label'];
    
    # Split the datasets into buildings and not buildings based on label counts
    blds = df.groupby('label').filter(lambda x: len(x) > second_layer_screen and len(x) < 20000);
    blds.reset_index(inplace=True);

    try:
        db = DBSCAN(eps=eps, min_samples=min_samples).fit(blds[['x', 'y']].as_matrix())
        blds = pd.DataFrame([blds.id, db.labels_]).T
        blds.columns = ['id', 'label'];
        blds['id'] = blds['id'].astype(int)
        blds['guess'] = 1.0;

        df = pd.merge(df[['id','is_building']], blds[['id','guess', 'label']], on='id', how='left')
        blds = None
        df.fillna(0, inplace=True)
        df['predict'] = abs(df.is_building - df.guess)

        if df.groupby('label').count().shape[0] >1:
            return 1 - df['predict'].sum()/ df['predict'].count(), df.groupby('label').count().shape[0]
        else:
            return None, None
    except:
        return None, None



                
if __name__ == '__main__':
    cursor = create_engine('postgresql://Mike@localhost:5432/building')
    connection = cursor.connect()
    df = pd.read_csv('../tests/data/CrossValidationData.csv')
    print dbscan(df, eps=3, min_samples=10,second_layer_screen=100, zboost=1.2);
    for eps in np.arange(2,5.0,0.5):
        for min_samples in np.arange(9, 12, 1.0):
            for second_layer_screen in np.arange(80, 120.0, 10.0):
                for zboost in np.arange(1., 1.3, 0.1):
                    print eps, min_samples,second_layer_screen, zboost
                    score, bld_count = dbscan(df, eps=eps, min_samples=min_samples,second_layer_screen=second_layer_screen, zboost=zboost);
                    if score:
                        connection.execute("INSERT INTO dbscan_cross VALUES(%(score)s, %(eps)f, %(min_samples)i, %(bld_count)i, %(second_layer_screen)i, %(zboost)f)" % {'score':score, 'eps':eps, 'min_samples':min_samples, "bld_count":int(bld_count), 'second_layer_screen':int(second_layer_screen), 'zboost': zboost})
    

