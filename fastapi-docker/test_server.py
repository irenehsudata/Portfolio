import requests

# query_character_by_id
# print(requests.get("http://127.0.0.1:8000/characters/0").json())

# query_character_by_parameter
print(requests.get("http://127.0.0.1:8000/characters?id=0").json())
