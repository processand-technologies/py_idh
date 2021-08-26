import json

def sub_transformer(entry, fix_structs = True):
    #print('sub transform', entry)
    if isinstance(entry, dict) and list(entry.keys()) == ['v']:
        return sub_transformer(list(entry.values())[0])
    elif fix_structs and isinstance(entry, dict) and list(entry.keys()) == ['f']:
        return {
            f'f_{index}': sub_transformer(sub_entry)
            for index, sub_entry in enumerate(list(entry.values())[0])}
    elif isinstance(entry, list):
        for index, sub_entry in enumerate(entry):
            entry[index] = sub_transformer(sub_entry)
    elif isinstance(entry, dict):
        for key, value in entry.items():
            entry[key] = sub_transformer(value)
    return entry

def transformer(entry, fix_structs):
    if isinstance(entry, str) and '{"v":' in entry:
        entry = sub_transformer(json.loads(entry), fix_structs)
    return entry

def fix_arrays_and_structs(df, fix_structs = True):
    return df.applymap(lambda x: transformer(x, fix_structs))
