# from py_kitab_diff import kitab_diff
# import json

# with open("input_data/maghribi_witnesses.json", encoding='utf-8') as f:
#     data = json.load(f)

# text_1, text_2, data_1, data_2 = kitab_diff(data[0]["text"], data[1]["text"], html_outfp="test_large_diff.html")


columns = [1, 2, 3, 4]
combinations = set()

for column in columns:
    combination = set()
    for column_2 in columns:
        combination.add(column_2)
    combinations.update(combination)

print(combinations)
print(len(combinations))

