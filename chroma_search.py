import json 
import chromadb

with open("movies.json", "r") as json_file:
    my_dict = json.load(json_file)

# print(my_dict[0]['_id']['$oid'])
# print(my_dict[0]['title'])
# print(my_dict[0]['plot'])
# print(my_dict[0]['fullplot'])

client = chromadb.PersistentClient(path='./')
collection = client.get_or_create_collection(name='movies')

def insert():
    global my_dict

    for i in range(100):
        # check if key title exists or not
        if 'title' not in my_dict[i]:
            continue
            
        if ('plot' not in my_dict[i] or 'fullplot' not in my_dict[i]):
            continue

        # check if key fullplot exists or not and assign the value to text 
        if 'fullplot' in my_dict[i]:
            text = f"Title: {my_dict[i]['title']} \nFull Plot: {my_dict[i]['fullplot']}"
        else:
            text = f"Title: {my_dict[i]['title']} \nPlot: {my_dict[i]['plot']}"

        # add the document to the collection
        collection.add(
            documents=[text],
            ids=[my_dict[i]['_id']['$oid'] ],
            metadatas=[
                {
                    # check if key exists or not and assign the value to the key
                    # ', '.join() is used to convert list to string
                    "languages": ', '.join(my_dict[i]['languages']) if 'languages' in my_dict[i] else "",
                    "genres": ', '.join(my_dict[i]['genres']) if 'genres' in my_dict[i] else "",
                    "runtime": int(my_dict[i]['runtime']['$numberInt']) if 'runtime' in my_dict[i] else 0,
                    "cast": ', '.join(my_dict[i]['cast']) if 'cast' in my_dict[i] else "",
                    "year": int(my_dict[i]['year']['$numberInt']) if 'year' in my_dict[i] else 0,
                    "countries": ', '.join(my_dict[i]['countries']) if 'countries' in my_dict[i] else "",
                    "writers": ', '.join(my_dict[i]['writers']) if 'writers' in my_dict[i] else "",
                    "directors": ', '.join(my_dict[i]['directors']) if 'directors' in my_dict[i] else "",
                    "released": my_dict[i]['released']['$date']['$numberLong'] if 'released' in my_dict[i] else "",
                }
            ]
        )

# insert()

result = collection.query(
    query_texts=["movie about india"],
    n_results=1
)
print(result)


'''
Filtering metadata supports the following operators:
    $eq - equal to (string, int, float)
    $ne - not equal to (string, int, float)
    $gt - greater than (int, float)
    $gte - greater than or equal to (int, float)
    $lt - less than (int, float)
    $lte - less than or equal to (int, float)

    $in - a value is in predefined list (string, int, float, bool)
    $nin - a value is not in predefined list (string, int, float, bool)
'''

result2 = collection.query(
    query_texts=["movie about world war one"],
    where={
        "directors": "John Ford"
    },
    n_results=2
)
print(result2)


result3 = collection.query(
    query_texts=["a movie where the lead actress dies"],
    where={
        "countries": {
            "$in": ["USA", "India", "UK"]
        }
    },
    n_results=2
)
print(result3)


result4 = collection.query(
    query_texts=["movie about love during war times"],
    where={
        "$and": [
            {
                "year": {
                    "$gte": 1910
                }
            },
            {
                "year": {
                    "$lte": 1920
                }
            }
        ]
    },
    n_results=2
)
print(result4)


result5 = collection.query(
    query_texts=["comedy movie to be watched with family"],
    where={
        "$or": [
            {
                "genres": {
                    "$in": ["Drama", "Comedy"]
                }
            },
            {
                "writers": {
                    "$in": ["William Shakespeare"]
                }
            }
        ]
    },
    n_results=2
)
print(result5)



'''
Filtering document text supports the following operators:
    $contains - a text contains a substring
    $ncontains - a text does not contain a substring
'''
result6 = collection.query(
    query_texts=["world war one"],
    where_document={
        "$contains": "unhappily married"
    },
    n_results=2
)
print(result6)
