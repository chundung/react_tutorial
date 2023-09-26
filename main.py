import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import pandas as pd

# Use a service account.
cred = credentials.Certificate('D:\DevEnv\python\config\credentials\hale-dynamo-399316-82e7ff82d889.json')

app = firebase_admin.initialize_app(cred)

db = firestore.client()

# docs = db.collection("top_data").stream()

# for doc in docs:
#     print(f"{doc.id} => {doc.to_dict()}")

chunk_size = 500  # maximum batch size for API

def lambda_funcB(d):
  arr=[]
  for v in d.values:
    docs = {
        "content_id": v[2],
        "service": v[3],
        "floor":v[4]
    }
    arr.append(docs)

  # print(d.values[0][0])
  # print(arr)
  doc = {
     d.types.values[0]: arr
  }
  # db.collection("top_data").document(d.values[0][0]).set(doc)

  # ebook:
  #   history:
  #     content_id: XXX,
  #     floor: XXX
  #   favorate:
  #     content_id: XXX,
  #     floor: XXX
  
  return d.values  


class Employee():
    def __init__(self, favorite, history):
        self.favorite = favorite
        self.history = history

def lambda_funcC(d):
  #  print(d.index)
  print("call func")
  print(d)
  print(d.index.names[0])

  member_id = []
  
  res =Employee([], [])
  for a in d.index:
    arr=[]
    vals = d.loc[[a]].values
    print(vals)
    for vw in vals:
      for v in vw:
        member_id.append(v[0])
        k =v[1]
        docs = {
            "content_id": v[2],
            "service": v[3],
            "floor":v[4]
        }
        arr.append(docs)

    setattr(res, k, arr)
  
  print(res.favorate)
  print(res.history)

  doc = {
     "favorite": res.favorate,
     "history": res.history
  }

  db.collection("top_data").document(member_id[0]).set(doc)
  return d.values

def lambda_func(d):
   document_id = d.types.values[0]
   for v in d.values:
      docs = {
         "content_id": v[2],
         "service": v[3],
         "floor":v[4]
      }

      db.collection("top_data").document(v[0]).collection(document_id).document().set(docs)
   return d.values

# loop through csv and insert rows in batches for firestore
with pd.read_csv('D:\DevEnv\python\data\sample.csv', chunksize=chunk_size) as reader:
    print(db.collection('top_data').document('M100001').get().to_dict())
    for chunk in reader:
      df = chunk.groupby(['member_id', 'types']).apply(lambda_funcB)
      df.groupby('member_id').apply(lambda_funcC)
      data_dict = chunk.to_dict()
      # print(data_dict['member_id'])
      # for record in data_dict:            
        #  print(data_dict[record])

      for index, row in chunk.iterrows():
         continue
        # print("index=", index)
        # print("row data:")
        # print(row['content_id'])
               
      # print(list(chunk.columns))
      # for row in chunk.values.tolist():
      #   print(row)
      #   print(type(row))
